import os
from pprint import pprint

import boto3
import botostubs

db_name = os.environ["DATABASE_NAME"]


if __name__ == "__main__":
    client: botostubs.Glue = boto3.client('glue')

    response = client.create_database(
        DatabaseInput={
            'Name': db_name,
        }
    )

    pprint(response)
