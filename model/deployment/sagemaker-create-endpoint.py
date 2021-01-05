from pathlib import Path

import boto3
import sagemaker
from cfn_tools import load_yaml
from sagemaker.sparkml.model import SparkMLModel

account = boto3.client('sts').get_caller_identity()['Account']
with open(Path(__file__).parents[2]/'setup/stack.yaml') as f:
    env = load_yaml(f)

file_name = "model.tar.gz"
bucket_name = env['Parameters']['PreBucket']['Default']
role = f'arn:aws:iam::{account}:role/sagemaker-role'
endpoint_name = env['Mappings']['TaskMap']['predict']['name']
model_name = "model"
instance_type = "ml.t2.medium"

if __name__ == "__main__":
    sparkml_model = SparkMLModel(model_data=f"s3://{bucket_name}/{file_name}",
                                 role=role,
                                 sagemaker_session=sagemaker.Session(
                                     boto3.session.Session()),
                                 name=model_name)

    sparkml_model.deploy(initial_instance_count=1,
                         instance_type=instance_type,
                         endpoint_name=endpoint_name)
