#!/bin/bash
set -e

BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')

# Upload glue job code
GLUE_DIR=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.DirMap.glue.name')
GLUE_JOB_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.parse.name')
GLUE_JOB_KEY="$GLUE_DIR"/"$GLUE_JOB_NAME"/glue_parser.py

aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key  "$GLUE_JOB_KEY" \
    --body parse-xml/glue_parser.py
