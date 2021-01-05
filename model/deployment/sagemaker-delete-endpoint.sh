#!/bin/bash

ENDPOINT_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.predict.name')
ENDPOINT_CONFIG_NAME="$ENDPOINT_NAME"

aws sagemaker delete-endpoint-config \
    --endpoint-config-name "$ENDPOINT_CONFIG_NAME"

aws sagemaker delete-endpoint \
    --endpoint-name "$ENDPOINT_NAME"
