import json
import os
import urllib.parse
import boto3

stepfunctions = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def lambda_handler(event, context):

    print("Received S3 event:")
    print(json.dumps(event))

    for record in event.get("Records", []):

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        print(f"S3 object received: s3://{bucket}/{key}")

        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({
                "bucket": bucket,
                "key": key
            })
        )

        print(f"Started Step Functions execution: {response['executionArn']}")

    return {
        "statusCode": 200,
        "body": json.dumps("Pipeline triggered successfully")
    }