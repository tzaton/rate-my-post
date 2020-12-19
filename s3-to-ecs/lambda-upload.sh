#!/bin/bash

export LAMBDA_NAME=s3-to-ecs
export LAMBDA_FILE=lambda_handler.py

export BUCKET_NAME=projekt-big-data-test
export LAMBDA_KEY=lambda/"$LAMBDA_NAME"/"$LAMBDA_FILE"

zip -j "$LAMBDA_NAME".zip "$LAMBDA_NAME"/"$LAMBDA_FILE"

aws s3api put-object \
--bucket "$BUCKET_NAME" \
--key  "$LAMBDA_KEY" \
--body "$LAMBDA_NAME".zip

rm "$LAMBDA_NAME".zip
