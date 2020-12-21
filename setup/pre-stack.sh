#!/bin/bash
set -e

# Create bucket
BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')

aws s3api create-bucket --bucket "$BUCKET_NAME"
aws s3api put-public-access-block \
    --bucket "$BUCKET_NAME" \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Upload lambda function code
. s3-to-ecs/lambda-upload.sh

# Upload glue job code
. parse-xml/glue-job-upload.sh

# Upload jars
JAR_DIR=$(cat setup/stack.yaml | cfn-flip | jq -r '.Mappings.DirMap.jars.name')

wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.11.0/spark-xml_2.11-0.11.0.jar

aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key  "$JAR_DIR"/spark-xml_2.11-0.11.0.jar \
    --body spark-xml_2.11-0.11.0.jar

rm spark-xml_2.11-0.11.0.jar
