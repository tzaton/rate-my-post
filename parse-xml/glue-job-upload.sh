#!/bin/bash

JOB_NAME="$TASK_PARSE_XML"
JOB_KEY="$GLUE_DIR"/"$JOB_NAME"/glue_parser.py

aws s3api put-object \
--bucket "$BUCKET_NAME" \
--key  "$JOB_KEY" \
--body "$TASK_PARSE_XML"/glue_parser.py
