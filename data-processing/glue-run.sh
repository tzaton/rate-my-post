#!/bin/bash
set -e

WORKFLOW_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.parse.name')

aws glue start-workflow-run --name "$WORKFLOW_NAME"
