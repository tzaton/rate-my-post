import json
import os
from pprint import pprint

import boto3
from botocore.exceptions import ClientError
import botostubs


if __name__ == "__main__":
    client: botostubs.IAM = boto3.client('iam')

    # ECS task - access S3
    role_name = os.environ['ECS_S3_ROLE']
    try:
        ecs_task = client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "ecs-tasks.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        pprint(ecs_task)
        policies = ['arn:aws:iam::aws:policy/AmazonS3FullAccess']
        for p in policies:
            response = client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=p
            )
    except ClientError as e:
        print(e)

    # Lambda - read S3 and run ECS
    role_name = os.environ['TASK_TRIGGER_UNZIP_ROLE']
    try:
        lambda_ecs_s3 = client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        pprint(lambda_ecs_s3)
        policies = ['arn:aws:iam::aws:policy/CloudWatchLogsFullAccess',
                    'arn:aws:iam::aws:policy/AmazonEC2ContainerServiceFullAccess',
                    'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess']
        for p in policies:
            response = client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=p
            )
    except ClientError as e:
        print(e)
