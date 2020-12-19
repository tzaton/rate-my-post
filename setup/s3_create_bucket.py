import sys
from pprint import pprint

import boto3
import botostubs


bucket_name = sys.argv[1]

if __name__ == "__main__":
    s3: botostubs.S3 = boto3.client('s3')

    response = s3.create_bucket(Bucket=bucket_name)
    pprint(response)

    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        })
