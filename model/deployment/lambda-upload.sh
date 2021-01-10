#!/bin/bash
set -e

export BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')

# Upload lambda function code
LAMBDA_DIR=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.DirMap.lambda.name')
export LAMBDA_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.TaskMap.triggerPredict.name')
export LAMBDA_KEY="$LAMBDA_DIR"/"$LAMBDA_NAME".zip

# Install extra packages
LAMBDA_VIRTUALENV=model/deployment/lambda-venv
rm -rf "$LAMBDA_VIRTUALENV"
virtualenv "$LAMBDA_VIRTUALENV"
source "$LAMBDA_VIRTUALENV"/bin/activate
pip install nltk
mkdir "$LAMBDA_VIRTUALENV"/nltk_data
python -m nltk.downloader -d "$LAMBDA_VIRTUALENV"/lib/python3.7/site-packages/nltk_data punkt averaged_perceptron_tagger stopwords
deactivate
cd "$LAMBDA_VIRTUALENV"/lib/python3.7/site-packages
zip -r ../../../../"$LAMBDA_NAME".zip .
cd ../../../../
zip -g "$LAMBDA_NAME".zip lambda_handler.py
cd ../../
rm -rf "$LAMBDA_VIRTUALENV"

aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key  "$LAMBDA_KEY" \
    --body model/deployment/"$LAMBDA_NAME".zip

rm model/deployment/"$LAMBDA_NAME".zip
