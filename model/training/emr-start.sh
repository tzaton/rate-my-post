#!/bin/bash
set -e

BUCKET_NAME=$(cat setup/stack.yaml | cfn-flip | jq -r '.Parameters.PreBucket.Default')

# upload bootstrap action script
aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key  bootstrap.sh \
    --body model/training/bootstrap.sh

BOOTSTRAP_PATH="$BUCKET_NAME"/bootstrap.sh

# start cluster
aws emr create-cluster \
    --applications Name=Hadoop Name=Livy Name=JupyterEnterpriseGateway Name=Spark \
    --release-label emr-5.32.0 \
    --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0f67eb2e","EmrManagedSlaveSecurityGroup":"sg-0e0d4f0cea3180c68","EmrManagedMasterSecurityGroup":"sg-0bee41df50951db6a"}' \
    --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --service-role EMR_DefaultRole \
    --configurations '[{"Classification":"livy-conf","Properties":{"livy.server.session.timeout":"8h"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --bootstrap-actions '[{"Path":"s3://'"$BOOTSTRAP_PATH"'","Name":"Install packages"}]' \
    --log-uri 's3n://'"$BUCKET_NAME"'/emr-logs/' \
    --enable-debugging \
    --visible-to-all-users \
    --no-termination-protected
