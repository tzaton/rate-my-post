export ACCOUNT_ID=246347629690

PROJECT_NAME=rate-my-post

export BUCKET_NAME="$PROJECT_NAME"
export ZIP_DIR=zip
export UNZIP_DIR=unzip
export LAMBDA_DIR=lambda

export REPO_NAME="$PROJECT_NAME"
export CLUSTER_NAME="$PROJECT_NAME"

export TASK_DOWNLOAD=zip-to-s3
export TASK_UNZIP=unzip-on-s3
export TASK_TRIGGER_UNZIP=s3-to-ecs

export TASK_TRIGGER_UNZIP_ROLE=lambda-get-s3-run-ecs
export ECS_S3_ROLE=ecs-run-task-s3