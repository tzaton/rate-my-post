import os

import boto3


def lambda_handler(event, context):
    # Get file key from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    print(f"Found S3 file: {bucket_name}/{file_name}")

    # Run ECS task
    ecs = boto3.client('ecs')
    cluster_name = os.environ['CLUSTER_NAME']
    task_family = os.environ['TASK_UNZIP']
    target_dir = os.environ['UNZIP_DIR']

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
                        target_dir,
                        '--overwrite'
                    ],
                }
            ]
        }
    )
