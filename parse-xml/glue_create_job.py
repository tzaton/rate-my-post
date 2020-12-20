import os
from pprint import pprint

import boto3
import botostubs

bucket_name = os.environ['BUCKET_NAME']
glue_dir = os.environ['GLUE_DIR']
jar_dir = os.environ['JAR_DIR']
data_dir = os.environ['DATA_DIR']
unzip_dir = os.environ['UNZIP_DIR']

job_name = os.environ['TASK_PARSE_XML']
job_role = os.environ['GLUE_S3_ROLE']

if __name__ == "__main__":
    glue: botostubs.Glue = boto3.client('glue')

    response = glue.create_job(
        Name=job_name,
        Description='Parse XML data and save to parquet',
        Role=job_role,
        ExecutionProperty={
            'MaxConcurrentRuns': 1
        },
        Command={
            'Name': 'glueetl',
            'ScriptLocation': f's3://{bucket_name}/{glue_dir}/{job_name}/glue_parser.py',
            'PythonVersion': '3'
        },
        DefaultArguments={
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-continuous-log-filter": "true",
            "--job-language": "python",
            "--extra-jars": f"s3://{bucket_name}/{jar_dir}/spark-xml_2.11-0.11.0.jar",
            "--bucket_name": bucket_name,
            "--input_dir": unzip_dir,
            "--output_dir": data_dir,
        },
        MaxRetries=0,
        GlueVersion='2.0',
        NumberOfWorkers=8,
        WorkerType='G.1X'
    )

    pprint(response)
