import json

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    SecretValue,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_redshift as redshift,
    aws_s3_assets as s3_assets,
    aws_secretsmanager as secretsmanager,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    triggers,
)
from constructs import Construct


class LoadRedshiftWithSfnStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.s3_file = s3_assets.Asset(
            self, "S3File", path=environment["FILE_PATH"]
        )

        self.lambda_redshift_access_role = iam.Role(
            self,
            "LambdaRedshiftAccessRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("redshift.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                ),  ### later principle of least privileges
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "redshift:GetClusterCredentials",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:BatchExecuteStatement",
                    "redshift-data:GetStatementResult",
                    "redshift-data:DescribeStatement",  # only needed for Trigger
                    "secretsmanager:GetSecretValue",  # needed for authenticating BatchExecuteStatement
                ],
                resources=["*"],
            ),
        )
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["states:SendTaskSuccess", "states:SendTaskFailure"],
                resources=["*"],
            ),
        )

        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[
                self.lambda_redshift_access_role.role_arn
            ],  # need IAM role for S3 COPY
            publicly_accessible=False,
        )

        self.redshift_secret = secretsmanager.Secret(
            self,
            "RedshiftSecret",
            secret_object_value={
                "username": SecretValue.unsafe_plain_text(environment["REDSHIFT_USER"]),
                "password": SecretValue.unsafe_plain_text(
                    environment["REDSHIFT_PASSWORD"]
                ),
            },
        )

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBTableForRedshiftQueries",
            partition_key=dynamodb.Attribute(
                name="full_table_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="utc_now_human_readable", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="delete_record_on",
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.dynamodb_table.add_global_secondary_index(
            index_name="is_still_processing_sql",  # hard coded
            partition_key=dynamodb.Attribute(
                name="redshift_queries_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="is_still_processing_sql?", type=dynamodb.AttributeType.STRING
            ),
        )

        self.configure_redshift_table_lambda = (
            _lambda.Function(  # will be used once in Trigger defined below
                self,  # create the schema and table in Redshift for DynamoDB CDC
                "ConfigureRedshiftTable",
                runtime=_lambda.Runtime.PYTHON_3_9,
                code=_lambda.Code.from_asset(
                    "lambda_code/configure_redshift_table_lambda",
                    exclude=[".venv/*"],
                ),
                handler="handler.lambda_handler",
                timeout=Duration.seconds(10),  # may take some time
                memory_size=128,  # in MB
                environment={
                    "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                    "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                    "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
                },
                role=self.lambda_redshift_access_role,
            )
        )
        self.truncate_and_load_redshift_table_lambda = _lambda.Function(
            self,
            "TruncateAndLoadRedshiftTable",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/truncate_and_load_redshift_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be instantaneous
            memory_size=128,  # in MB
            environment={
                "DYNAMODB_TTL_IN_DAYS": json.dumps(environment["DYNAMODB_TTL_IN_DAYS"]),
                "FILE_TYPE": environment["FILE_TYPE"],
                "REDSHIFT_COPY_ADDITIONAL_ARGUMENTS": environment["REDSHIFT_COPY_ADDITIONAL_ARGUMENTS"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
            },
            role=self.lambda_redshift_access_role,
        )
        self.redshift_queries_finished_lambda = _lambda.Function(
            self,  # create the schema and table in Redshift for DynamoDB CDC
            "RedshiftQueriesFinished",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/redshift_queries_finished_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be instantaneous
            memory_size=128,  # in MB
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )

        # Step Function definition
        truncate_and_load_redshift_table = sfn_tasks.LambdaInvoke(
            self,
            "truncate_and_load_redshift_table",
            lambda_function=self.truncate_and_load_redshift_table_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({"task_token": sfn.JsonPath.task_token}),
            timeout=Duration.minutes(
                environment["REDSHIFT_QUERY_MAX_RUNTIME_LIMIT_IN_MINUTES"]
            ),
            retry_on_service_exceptions=False,
        )
        self.state_machine = sfn.StateMachine(
            self,
            "truncate_and_load_redshift_table_with_task_token",
            definition=truncate_and_load_redshift_table,
        )

        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEvery1Minute",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda = events.Rule(
            self,
            "EventRuleToTriggerRedshiftQueriesFinishesLambda",
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda.add_event_pattern(
            source=["aws.redshift-data"],
            # resources=[{"suffix": self.redshift_cluster.ref}],
            # resources=["arn:aws:redshift:us-east-1:...:cluster:redshiftcluster-u94ov1ux9grq"],
            detail={
                "principal": [
                    {
                        "suffix": self.truncate_and_load_redshift_table_lambda.function_name
                    }
                ],
                "statementId": [{"exists": True}],
                "state": [{"exists": True}],
            },
            # detail=["arn:aws:sts::...:assumed-role/LoadRedshiftWithSfnStack-LambdaRedshiftFullAccessR-1BCCVKE7JB2LH/LoadRedshiftWithSfnStack-TruncateAndLoadRedshiftTa-NhdJFrmC13Nl"],
            detail_type=["Redshift Data Statement Status Change"],
        )

        # connect the AWS resources
        self.configure_redshift_table_lambda.add_environment(
            key="REDSHIFT_ENDPOINT_ADDRESS",
            value=self.redshift_cluster.attr_endpoint_address,
        )
        self.configure_redshift_table_lambda.add_environment(
            key="REDSHIFT_SECRET_ARN",
            value=self.redshift_secret.secret_arn,
        )
        self.trigger_configure_redshift_table_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRedshiftTableLambda",
            handler=self.configure_redshift_table_lambda,  # this is underlying Lambda
            execute_after=[self.redshift_cluster],  # runs once after RDS creation
            execute_before=[self.scheduled_eventbridge_event],
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_rds_lambda.timeout,
            # execute_on_handler_change=False,
        )
        self.truncate_and_load_redshift_table_lambda.add_environment(
            key="REDSHIFT_ENDPOINT_ADDRESS",
            value=self.redshift_cluster.attr_endpoint_address,
        )
        self.truncate_and_load_redshift_table_lambda.add_environment(
            key="REDSHIFT_ROLE",
            value=self.lambda_redshift_access_role.role_arn,
        )
        self.truncate_and_load_redshift_table_lambda.add_environment(
            key="REDSHIFT_SECRET_ARN",
            value=self.redshift_secret.secret_arn,
        )
        self.truncate_and_load_redshift_table_lambda.add_environment(
            key="S3_FILENAME",
            value=self.s3_file.s3_object_url,
        )
        self.truncate_and_load_redshift_table_lambda.add_environment(
            key="DYNAMODB_TABLE", value=self.dynamodb_table.table_name
        )
        self.dynamodb_table.grant_read_write_data(
            self.truncate_and_load_redshift_table_lambda
        )
        self.redshift_queries_finished_lambda.add_environment(
            key="DYNAMODB_TABLE", value=self.dynamodb_table.table_name
        )

        self.scheduled_eventbridge_event.add_target(
            target=events_targets.SfnStateMachine(
                machine=self.state_machine,
                retry_attempts=0,
                # input=None, role=None,
                ### then put in DLQ
            ),
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda.add_target(
            events_targets.LambdaFunction(
                handler=self.redshift_queries_finished_lambda,
                # retry_attempts=0,  ### doesn't seem to do anything
                ### then put in DLQ
            )
        )

        # write Cloudformation Outputs
        self.output_s3_bucket_name = CfnOutput(
            self, "S3BucketName", value=self.s3_file.s3_bucket_name
        )
        self.output_s3_object_key = CfnOutput(
            self, "S3ObjectKey", value=self.s3_file.s3_object_key
        )
        self.output_s3_object_url = CfnOutput(
            self, "S3ObjectURL", value=self.s3_file.s3_object_url
        )
        self.output_redshift_endpoint_address = CfnOutput(
            self,
            "RedshiftEndpointAddress",  # Output omits underscores and hyphens
            value=self.redshift_cluster.attr_endpoint_address,
        )
