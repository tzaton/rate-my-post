import os
from pprint import pprint

import boto3
import botostubs

repo_name = os.environ["REPO_NAME"]


if __name__ == "__main__":
    ecr: botostubs.ECR = boto3.client('ecr')

    response = ecr.create_repository(
            repositoryName=repo_name
        )

    pprint(response)
