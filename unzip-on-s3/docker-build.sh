#!/bin/bash
set -e

ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
REGION=$(aws configure get region)
ECR="$ACCOUNT_ID".dkr.ecr."$REGION".amazonaws.com
REPO_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.unzip.name')
IMAGE_NAME="$ECR"/"$REPO_NAME"

aws ecr get-login-password | docker login --username AWS --password-stdin "$ECR"
# docker build -f unzip-on-s3/Dockerfile -t "$IMAGE_NAME":latest .
docker push "$IMAGE_NAME":latest
