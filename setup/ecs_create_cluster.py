import sys
from pprint import pprint

import boto3
import botostubs

cluster_name = sys.argv[1]


if __name__ == "__main__":
    ecs: botostubs.ECS = boto3.client('ecs')

    response = ecs.create_cluster(
        clusterName=cluster_name,
        capacityProviders=[
            'FARGATE',
        ]
    )

    pprint(response)
