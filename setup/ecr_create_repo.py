import sys
from pprint import pprint

import boto3
import botostubs

repo_name = sys.argv[1]


if __name__ == "__main__":
    ecr: botostubs.ECR = boto3.client('ecr')

    response = ecr.create_repository(
            repositoryName=repo_name
        )

    pprint(response)
