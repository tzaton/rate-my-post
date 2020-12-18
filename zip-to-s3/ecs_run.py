from pprint import pprint

import boto3
import botostubs
from src.arg_parser import get_parser

task_family = "zip-to-s3"
cluster_name = "rate-my-post"


if __name__ == "__main__":

    argparser = get_parser()
    argparser.add_argument("--parent-url",
                           help="Parent URL to download from",
                           required=True)
    args = argparser.parse_args()

    ecs: botostubs.ECS = boto3.client('ecs')

    response = ecs.run_task(
        taskDefinition=task_family,
        cluster=cluster_name,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    'subnet-bbe95be4',
                ],
                'assignPublicIp': 'ENABLED'}
        },
        overrides={
            'containerOverrides': [
                {
                    'name': task_family,
                    'environment': [
                        {
                            'name': 'DATA_PARENT_URL',
                            'value': args.parent_url
                        }
                    ],
                    'command': [
                        '--dataset-name',
                        ' '.join(args.dataset_name),
                        '--local-dir',
                        args.local_dir,
                        '--s3-bucket',
                        args.s3_bucket,
                        '--s3-dir',
                        args.s3_dir,
                        '--chunk-size',
                        args.chunk_size,
                        '--logging-level',
                        args.logging_level
                    ],
                }
            ]
        }
    )

    pprint(response)
