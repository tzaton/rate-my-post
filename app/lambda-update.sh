#!/bin/bash
set -e

. app/lambda-upload.sh

aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --s3-bucket "$BUCKET_NAME" \
    --s3-key "$LAMBDA_KEY" \
