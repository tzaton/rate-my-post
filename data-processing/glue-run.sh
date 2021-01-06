#!/bin/bash
set -e

WORKFLOW_NAME=data-processing

aws glue start-workflow-run \
    --name "$WORKFLOW_NAME"
