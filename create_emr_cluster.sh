aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Hue Name=Spark --ec2-attributes '{"KeyName":"ansible"
,"InstanceProfile":"EMR_EC2_DefaultRole"
,"SubnetId":"subnet-fa244281"
,"EmrManagedSlaveSecurityGroup":"sg-0135b3df8d661d1d2"
,"EmrManagedMasterSecurityGroup":"sg-0c1d0b2ffc80abe05"}' --release-label emr-6.2.0 --log-uri 's3n://aws-logs-198373991235-eu-central-1/elasticmapreduce/' --steps '[{"Args":["spark-submit"
,"--deploy-mode"
,"cluster"
,"--packages"
,"com.databricks:spark-xml_2.12:0.11.0"
,"s3://data-emr-analytics/bin/spark-job.py"]
,"Type":"CUSTOM_JAR"
,"ActionOnFailure":"CONTINUE"
,"Jar":"command-runner.jar"
,"Properties":""
,"Name":"Applicazione Spark"}]' --instance-groups '[{"InstanceCount":2
,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32
,"VolumeType":"gp2"}
,"VolumesPerInstance":2}]}
,"InstanceGroupType":"CORE"
,"InstanceType":"m5.xlarge"
,"Name":"Core (principale) - 2"}
,{"InstanceCount":1
,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32
,"VolumeType":"gp2"}
,"VolumesPerInstance":2}]}
,"InstanceGroupType":"MASTER"
,"InstanceType":"m5.xlarge"
,"Name":"Master - 1"}]' --configurations '[{"Classification":"spark-env"
,"Properties":{}
,"Configurations":[{"Classification":"export"
,"Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]}
,{"Classification":"hive-site"
,"Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' --auto-terminate --auto-scaling-role EMR_AutoScaling_DefaultRole --bootstrap-actions '[{"Path":"s3://data-emr-analytics/bin/bootstrap.sh"
,"Name":"Operazione personalizzata"}]' --ebs-root-volume-size 100 --service-role EMR_DefaultRole --enable-debugging --name 'spark-test' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region eu-central-1
