#!/bin/bash
set -e

export BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')

# Upload lambda function code
LAMBDA_DIR=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.DirMap.lambda.name')
export LAMBDA_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.triggerUnzip.name')
export LAMBDA_KEY="$LAMBDA_DIR"/"$LAMBDA_NAME"/lambda_handler.py

zip -j "$LAMBDA_NAME".zip s3-to-ecs/lambda_handler.py

aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key  "$LAMBDA_KEY" \
    --body "$LAMBDA_NAME".zip

rm "$LAMBDA_NAME".zip
