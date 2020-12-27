#!/bin/bash
set -e

BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.AppBucket.Default')

# Upload website
aws s3 cp app/src s3://"$BUCKET_NAME"/ --recursive
