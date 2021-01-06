#!/bin/bash
set -e

# Create buckets
PRE_BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')
APP_BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.AppBucket.Default')

# pre
aws s3api create-bucket --bucket "$PRE_BUCKET_NAME"
aws s3api put-public-access-block \
    --bucket "$PRE_BUCKET_NAME" \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# app
aws s3api create-bucket --bucket "$APP_BUCKET_NAME"
aws s3api put-public-access-block \
    --bucket "$APP_BUCKET_NAME" \
    --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"
aws s3api put-bucket-policy --bucket "$APP_BUCKET_NAME" --policy file://app/policy.json

# Upload website
. app/website-upload.sh
aws s3 website s3://"$APP_BUCKET_NAME"/ --index-document index.html --error-document error.html

# Upload lambda functions
. data-import/s3-to-ecs/lambda-upload.sh
. model/deployment/lambda-upload.sh

# Upload glue jobs
. data-processing/parse-xml/glue-job-upload.sh
. data-processing/feature-engineering/glue-job-upload.sh
. data-processing/get-features/glue-job-upload.sh

# Upload jars
JAR_DIR=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.DirMap.jars.name')

wget -P jars https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.11.0/spark-xml_2.11-0.11.0.jar
wget -P jars https://repo1.maven.org/maven2/com/johnsnowlabs/nlp/spark-nlp_2.11/2.6.2/spark-nlp_2.11-2.6.2.jar
wget -P jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.930/aws-java-sdk-core-1.11.930.jar

aws s3 cp jars s3://"$BUCKET_NAME"/"$JAR_DIR" --recursive
