#!/bin/bash

ACCOUNT_ID=$(cat setup/config.json | jq -r '.ACCOUNT_ID')
IMAGE_NAME=unzip-on-s3

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$ACCOUNT_ID".dkr.ecr.us-east-1.amazonaws.com
docker build -f unzip-on-s3/Dockerfile -t "$ACCOUNT_ID".dkr.ecr.us-east-1.amazonaws.com/"$IMAGE_NAME":latest .
docker push "$ACCOUNT_ID".dkr.ecr.us-east-1.amazonaws.com/"$IMAGE_NAME":latest
