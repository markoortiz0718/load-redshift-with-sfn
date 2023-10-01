#!/usr/bin/env python3
import aws_cdk as cdk
import boto3

from load_redshift_with_sfn.load_redshift_with_sfn_stack import LoadRedshiftWithSfnStack


app = cdk.App()
environment = app.node.try_get_context("environment")
account = boto3.client("sts").get_caller_identity()["Account"]
LoadRedshiftWithSfnStack(
    app,
    "LoadRedshiftWithSfnStack",
    env=cdk.Environment(account=account, region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
