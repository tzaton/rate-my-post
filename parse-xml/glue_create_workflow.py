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
    workflow_name = task

    workflow = glue.create_workflow(
        Name=workflow_name,
        Description='Parse XML and update metastore',
        MaxConcurrentRuns=1
    )

    start = glue.create_trigger(
        Name='start-parser',
        WorkflowName=workflow_name,
        Type='ON_DEMAND',
        Actions=[
            {
                'JobName': task,
            },
        ],
        Description='Start XML parsing',
        StartOnCreation=False,
    )

    for t in tables:
        trigger = glue.create_trigger(
            Name=f'update-{t}',
            WorkflowName=workflow_name,
            Type='CONDITIONAL',
            Predicate={
                'Logical': 'AND',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'JobName': task,
                        'State': 'SUCCEEDED',
                    },
                ]
            },
            Actions=[
                {
                    'CrawlerName': f"{task}-{t}"
                },
            ],
            StartOnCreation=True,
        )
