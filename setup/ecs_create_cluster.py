import os
from pprint import pprint

import boto3
import botostubs

cluster_name = os.environ["CLUSTER_NAME"]


if __name__ == "__main__":
    ecs: botostubs.ECS = boto3.client('ecs')

    response = ecs.create_cluster(
        clusterName=cluster_name,
        capacityProviders=[
            'FARGATE',
        ]
    )

    pprint(response)
