export ACCOUNT_ID=246347629690

PROJECT_NAME=rate-my-post

# S3
export BUCKET_NAME="$PROJECT_NAME"
export ZIP_DIR=zip
export UNZIP_DIR=unzip
export DATA_DIR=data
export LAMBDA_DIR=lambda
export GLUE_DIR=glue
export JAR_DIR=jars

# ECR
export REPO_NAME="$PROJECT_NAME"

# ECS
export CLUSTER_NAME="$PROJECT_NAME"

# GLUE
export DATABASE_NAME=default

# Tasks
export TASK_DOWNLOAD=zip-to-s3
export TASK_UNZIP=unzip-on-s3
export TASK_TRIGGER_UNZIP=s3-to-ecs
export TASK_PARSE_XML=parse-xml

# IAM Roles
export TASK_TRIGGER_UNZIP_ROLE=lambda-get-s3-run-ecs
export ECS_S3_ROLE=ecs-run-task-s3
export GLUE_S3_ROLE=glue-run-job-s3
