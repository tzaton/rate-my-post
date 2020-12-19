from pprint import pprint

import boto3
import botostubs
from botocore.exceptions import ClientError


def create_log_group(group_name):
    logs: botostubs.CloudWatchLogs = boto3.client('logs')
    try:
        response = logs.create_log_group(logGroupName=group_name)
        pprint(response)
    except ClientError as e:
        print(e)
