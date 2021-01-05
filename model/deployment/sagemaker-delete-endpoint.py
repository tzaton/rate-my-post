from pathlib import Path

import boto3
from cfn_tools import load_yaml

client = boto3.client('sagemaker')
with open(Path(__file__).parents[2]/'setup/stack.yaml') as f:
    env = load_yaml(f)

endpoint_name = env['Mappings']['TaskMap']['predict']['name']

if __name__ == "__main__":
    client.delete_endpoint(
        EndpointName=endpoint_name
    )
