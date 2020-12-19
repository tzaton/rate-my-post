import os
from pprint import pprint

import boto3
import botostubs


if __name__ == "__main__":
    bucket_name = os.environ["BUCKET_NAME"]
    lambda_path = os.path.join(os.environ["LAMBDA_DIR"],
                               os.environ["TASK_TRIGGER_UNZIP"],
                               "lambda_handler.py")
    function_name = os.environ["TASK_TRIGGER_UNZIP"]
    role_name = os.environ['TASK_TRIGGER_UNZIP_ROLE']
    trigger_prefix = os.environ["ZIP_DIR"]+"/"

    aws_lambda: botostubs.Lambda = boto3.client('lambda')
    iam: botostubs.IAM = boto3.client('iam')
    s3: botostubs.S3 = boto3.client('s3')

    response = aws_lambda.create_function(
        FunctionName=function_name,
        Runtime='python3.7',
        Role=iam.get_role(RoleName=role_name)['Role']['Arn'],
        Handler='lambda_handler.lambda_handler',
        Code={
            'S3Bucket': bucket_name,
            'S3Key': lambda_path,
        },
        Description='Run ECS task on S3 file',
        Timeout=10,
        MemorySize=128,
        Publish=True,
        Environment={
            'Variables': {
                'CLUSTER_NAME': os.environ['CLUSTER_NAME'],
                'TASK_UNZIP': os.environ['TASK_UNZIP'],
                'UNZIP_DIR': os.environ['UNZIP_DIR']
            }
        },
    )
    pprint(response)

    # Add invoke permission
    response = aws_lambda.add_permission(
        FunctionName=function_name,
        StatementId="S3StatementId",
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=f'arn:aws:s3:::{bucket_name}'
    )
    pprint(response)

    # Add event notification for trigger
    response = s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': aws_lambda.get_function(FunctionName=function_name)['Configuration']['FunctionArn'],
                    'Events': [
                        's3:ObjectCreated:*'
                    ],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': trigger_prefix
                                },
                            ]
                        }
                    }
                },
            ]
        }
    )
    pprint(response)
