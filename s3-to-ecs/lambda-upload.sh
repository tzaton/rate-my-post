#!/bin/bash

export LAMBDA_NAME="$TASK_TRIGGER_UNZIP"
export LAMBDA_KEY="$LAMBDA_DIR"/"$LAMBDA_NAME"/lambda_handler.py

zip -j "$LAMBDA_NAME".zip "$LAMBDA_NAME"/lambda_handler.py

aws s3api put-object \
--bucket "$BUCKET_NAME" \
--key  "$LAMBDA_KEY" \
--body "$LAMBDA_NAME".zip

rm "$LAMBDA_NAME".zip
