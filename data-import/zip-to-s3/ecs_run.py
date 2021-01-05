from pathlib import Path

import boto3
from cfn_tools import load_yaml
from src.arg_parser import get_parser


with open(Path(__file__).parents[2]/'setup/stack.yaml') as f:
    env = load_yaml(f)
task_family = env['Mappings']['TaskMap']['upload']['name']
cluster_name = env['Parameters']['ProjectName']['Default']

if __name__ == "__main__":

    argparser = get_parser()
    argparser.add_argument("--parent-url",
                           help="Parent URL to download from",
                           required=True)
    args = argparser.parse_args()

    ecs = boto3.client('ecs')

    # Get VPC subnets
    ec2 = boto3.client('ec2')
    subnets = [sn['SubnetId'] for sn in ec2.describe_subnets()['Subnets']]

    response = ecs.run_task(
        taskDefinition=task_family,
        cluster=cluster_name,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnets,
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
                        '-n',
                        args.n,
                        '--logging-level',
                        args.logging_level,
                        '--overwrite' if args.overwrite else '--no-overwrite'
                    ],
                }
            ]
        }
    )
