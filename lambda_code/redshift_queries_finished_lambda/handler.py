import json
import os

import boto3

dynamodb_table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE"])
redshift_data_client = boto3.client("redshift-data")
sfn_client = boto3.client("stepfunctions")


def lambda_handler(event, context) -> None:
    # print("event", event)
    redshift_queries_state = event["detail"]["state"]
    if redshift_queries_state in ["SUBMITTED", "PICKED", "STARTED"]:
        print(f"Redshift state is {redshift_queries_state}, so ignore")
        return
    redshift_queries_id = event["detail"]["statementId"]

    response = dynamodb_table.query(
        IndexName="is_still_processing_sql",  # hard coded
        KeyConditionExpression=boto3.dynamodb.conditions.Key("redshift_queries_id").eq(
            redshift_queries_id
        ),
        # Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
        # AttributesToGet=['string'],
    )
    assert response["Count"] == 1, (
        f'For `redshift_queries_id` "{redshift_queries_id}", there should be exactly 1 record '
        f"but got {response['Count']} records. The records are: {response['Items']}"
    )
    record = response["Items"][0]
    task_token = record["task_token"]
    if redshift_queries_state == "FINISHED":
        sfn_client.send_task_success(
            taskToken=task_token,
            output='"json output of the task"',  # figure out what to write here such as completed statementId, table name
        )
        num_queries = len(
            json.loads(record["sql_queries"])
        )  # assumes that 'select count(*)' is last query
        row_count = redshift_data_client.get_statement_result(
            Id=f"{redshift_queries_id}:{num_queries}"
        )["Records"][0][0]["longValue"]
        dynamodb_table.update_item(
            Key={
                "full_table_name": record["full_table_name"],
                "utc_now_human_readable": record["utc_now_human_readable"],
            },
            # REMOVE is more important than SET. REMOVE deletes attribute that
            # makes record show up in the table's GSI.
            UpdateExpression="REMOVE #isp SET row_count = :rc",
            ExpressionAttributeValues={":rc": row_count},
            ExpressionAttributeNames={"#isp": "is_still_processing_sql?"},
        )
    elif redshift_queries_state in ["ABORTED", "FAILED"]:
        sfn_client.send_task_failure(
            taskToken=task_token,
            error='"string"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
    else:  # 'ALL'
        sfn_client.send_task_failure(
            taskToken=task_token,
            error=f'"`redshift_queries_state` is {redshift_queries_state}"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
