import os
import sys
from pprint import pprint
from pathlib import Path

import boto3
import botostubs

sys.path.insert(0, str(Path(__file__).parents[1]/"setup"))
from create_log_group import create_log_group

task_family = os.environ['TASK_UNZIP']
container_name = os.environ['TASK_UNZIP']

ACCOUNT_ID = os.environ["ACCOUNT_ID"]
image = f"{ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/{container_name}"

role = os.environ['ECS_S3_ROLE']


if __name__ == "__main__":
    ecs: botostubs.ECS = boto3.client('ecs')
    log_group = f'/ecs/{task_family}'
    create_log_group(log_group)

    response = ecs.register_task_definition(
        family=task_family,
        taskRoleArn=role,
        executionRoleArn="ecsTaskExecutionRole",
        networkMode='awsvpc',
        requiresCompatibilities=[
            'FARGATE',
        ],
        cpu='1vCPU',
        memory='2GB',
        containerDefinitions=[
            {
                'name': container_name,
                'image': image,
                'essential': True,
                'logConfiguration': {
                    'logDriver': 'awslogs',
                    'options': {
                        'awslogs-group': log_group,
                        'awslogs-region': 'us-east-1',
                        'awslogs-stream-prefix': 'ecs'
                    },
                }
            }
        ],
    )

    pprint(response)
