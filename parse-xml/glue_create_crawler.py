import os

import boto3
import botostubs


tables = [
    'badges',
    'comments',
    'post_history',
    'post_links',
    'posts',
    'tags',
    'users',
    'votes',
]

bucket_name = os.environ["BUCKET_NAME"]
data_dir = os.environ["DATA_DIR"]
db_name = os.environ["DATABASE_NAME"]
task = os.environ["TASK_PARSE_XML"]
role = os.environ["GLUE_S3_ROLE"]

if __name__ == "__main__":
    glue: botostubs.Glue = boto3.client('glue')

    for t in tables:
        response = glue.create_crawler(
            Name=f"{task}-{t}",
            Role=role,
            DatabaseName=db_name,
            Description=f'Crawl table: {t} (parsed from XML)',
            Targets={
                'S3Targets': [
                    {
                        'Path': f's3://{bucket_name}/{data_dir}/{t}',
                    },
                ],
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DELETE_FROM_DATABASE'
            },
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            LineageConfiguration={
                'CrawlerLineageSettings': 'DISABLE'
            },
        )
