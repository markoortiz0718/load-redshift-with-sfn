import json
import os
from datetime import datetime, timedelta

import boto3


dynamodb_table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE"])
redshift_data_client = boto3.client("redshift-data")

FILE_TYPE = os.environ["FILE_TYPE"]
DYNAMODB_TTL_IN_DAYS = json.loads(os.environ["DYNAMODB_TTL_IN_DAYS"])
REDSHIFT_COPY_ADDITIONAL_ARGUMENTS = os.environ["REDSHIFT_COPY_ADDITIONAL_ARGUMENTS"]
# aws_redshift.CfnCluster(...).attr_id (for cluster name) is broken, so using endpoint address instead
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(".")[0]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_ROLE = os.environ["REDSHIFT_ROLE"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]
S3_FILENAME = os.environ["S3_FILENAME"]


def lambda_handler(event, context) -> None:
    # print(f"event: {event}")
    assert (
        "task_token" in event
    ), f'"task_token" key must be in `event`. `event` is: {event}'
    sql_queries = [
        f"truncate {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME};",
        f"""
        copy {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}
        from '{S3_FILENAME}'
        iam_role '{REDSHIFT_ROLE}'
        format as {FILE_TYPE} {REDSHIFT_COPY_ADDITIONAL_ARGUMENTS};
        """,  # eventually put REDSHIFT_DATABASE_NAME, REDSHIFT_SCHEMA_NAME, REDSHIFT_TABLE_NAME as event payload
        f"select count(*) from {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME};",
    ]
    response = redshift_data_client.batch_execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        SecretArn=REDSHIFT_SECRET_ARN,
        Database=REDSHIFT_DATABASE_NAME,
        Sqls=sql_queries,
        WithEvent=True,
    )
    # print(response)
    response.pop(
        "CreatedAt", None
    )  # has a datetime() object that is not JSON serializable
    utc_now = datetime.utcnow()
    records_expires_on = utc_now + timedelta(days=DYNAMODB_TTL_IN_DAYS)
    dynamodb_table.put_item(
        Item={
            "full_table_name": f"{REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}",  # pk in primary index
            "utc_now_human_readable": utc_now.strftime("%Y-%m-%d %H:%M:%S")
            + " UTC",  # sk in primary index
            "redshift_queries_id": response["Id"],  # pk in GSI
            "is_still_processing_sql?": "yes",  # sk in GSI
            "task_token": event["task_token"],
            "sql_queries": json.dumps(sql_queries),
            "redshift_response": json.dumps(response),
            "delete_record_on": int(records_expires_on.timestamp()),
            "delete_record_on_human_readable": records_expires_on.strftime(
                "%Y-%m-%d %H:%M:%S" + " UTC"
            ),
        }
    )
