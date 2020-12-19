import botostubs
from pprint import pprint

import boto3


def lambda_handler(event, context):
    # Get file key from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    print(f"Found S3 file: {bucket_name}/{file_name}")

    # Run ECS task
    cluster_name = "rate-my-post"
    task_family = "unzip-on-s3"
    ecs = boto3.client('ecs')
    target_dir = 'unzip'

    # Get VPC subnets
    ec2 = boto3.client('ec2')
    subnets = [sn['SubnetId'] for sn in ec2.describe_subnets()['Subnets']]

    print(f"Starting ECS task: {task_family}")
    response = ecs.run_task(
        taskDefinition=task_family,
        cluster=cluster_name,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnets,
                'assignPublicIp': 'ENABLED'}
        },
        overrides={
            'containerOverrides': [
                {
                    'name': task_family,
                    'command': [
                        '--dataset-name',
                        file_name,
                        '--local-dir',
                        '/tmp',
                        '--s3-bucket',
                        bucket_name,
                        '--s3-dir',
                        target_dir
                    ],
                }
            ]
        }
    )

    pprint(response)
