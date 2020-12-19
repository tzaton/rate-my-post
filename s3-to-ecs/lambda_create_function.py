from pprint import pprint

import boto3
import botostubs


if __name__ == "__main__":
    bucket_name = 'projekt-big-data-test'
    lambda_path = "lambda/s3-to-ecs/lambda_handler.py"
    function_name = 's3-to-ecs'
    role_name = 'lambda-get-s3-run-ecs'
    trigger_prefix = 'zip/'

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
