#!/bin/bash
set -e

BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.AppBucket.Default')

# Upload website
aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key index.html \
    --body app/src/index.html \
    --content-type text/html
