# A simple ETL using Spark on EMR ( AWS )

After installing the AWS CLI, 
configure it to use your credentials.\
To install AWS CLI, please refer to https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html

```shell

$ aws configure
AWS Access Key ID [None]: <YOUR_AWS_ACCESS_KEY_ID>
AWS Secret Access Key [None]: <YOUR_AWS_SECRET_ACCESS_KEY>
Default region name [None]: <YOUR_AWS_REGION>
Default output format [None]: json

```

# Assign aws user all necessary permission\
As this is just a demo you can assign full access role for:\
EMR,rds and s3 services.\
but in a production enviroment more stricted permissions should be granted

```shell

$ aws configure
AWS Access Key ID [None]: <YOUR_AWS_ACCESS_KEY_ID>
AWS Secret Access Key [None]: <YOUR_AWS_SECRET_ACCESS_KEY>
Default region name [None]: <YOUR_AWS_REGION>
Default output format [None]: json

```

# Create an s3 bucket and upload all necessary files to it

```shell

a) aws s3 create-bucket --bucket data-emr-analytics --region eu-central-1
b) aws s3 cp movies_metadata.csv s3://data-emr-analytics/csv/ 
c) aws s3 cp enwiki-latest-abstract.xml  s3://data-emr-analytics/xml/
d) aws s3 cp bootstrap.sh s3://data-emr-analytics/bin/
e) aws s3 cp spark-job.py s3://data-emr-analytics/bin/


```


# Create a postgres rds 

```shell

aws rds create-db-instance
    --db-name postgres
    --db-instance-identifier data-analytics.cvilmwwj2hq5.eu-central-1.rds.amazonaws.com
    --allocated-storage 30
    --db-instance-class db.t3.micro \
    --engine postgres \
    --master-username postgres \
    --master-user-password <password> \
    --vpc-security-group-ids <vpc security group for rds> \
    --db-subnet-group <subnet group e.g. default-yourvpcid> \
    --availability-zone <> \
    --port 5432
```

# Launch emr cluster

```shell

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

```


## Connect to postgres database and check that data have been loaded


```shell
psql -h data-analytics.cvilmwwj2hq5.eu-central-1.rds.amazonaws.com -U postgres -d postgres

postgres=>select * from top1000 limit 10;

       title       |  budget  | year | revenue  | popularity |       ratio        |                                                                 companieslist                                                                  |                        url                         |                                                                                                                                                                                        abstract                                           
-------------------+----------+------+----------+------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Very Bad Things   | 30000000 | 1998 |  9898412 |  10.432293 | 3.0307891811332968 | Initial Entertainment Group (IEG)-Interscope Communications-VBT Productions-Ballpark Productions Partnership                                   | https://en.wikipedia.org/wiki/Very_Bad_Things      | | writer     = Peter Berg
 Hellboy           | 66000000 | 2004 | 99318987 |  13.206854 | 0.6645255050778961 | Columbia Pictures-Revolution Studios-Dark Horse Entertainment-Lawrence Gordon Productions-Starlite Films                                       | https://en.wikipedia.org/wiki/Hellboy              | | creators       = Mike Mignola
 Whatever It Takes | 15000000 | 2000 |  9902115 |   3.550711 | 1.5148278928289562 | Columbia Pictures Corporation                                                                                                                  | https://en.wikipedia.org/wiki/Whatever_It_Takes    | Whatever It Takes may refer to:
 Black Mass        | 53000000 | 2015 | 98837872 |   10.61072 | 0.5362316987156502 | Infinitum Nihil-Head Gear Films-Cross Creek Pictures-Free State Pictures-RatPac-Dune Entertainment-Le Grisbi Productions-Vendian Entertainment | https://en.wikipedia.org/wiki/Black_Mass           | A Black Mass is a ceremony typically celebrated by various satanic groups. It has existed for centuries in different forms and is directly based on a Catholic Mass.
 Jade              | 50000000 | 1995 |  9851610 |  11.418917 |    5.0753125631242 | Paramount Pictures                                                                                                                             | https://en.wikipedia.org/wiki/Jade                 | Jade is an ornamental mineral, mostly known for its green varieties, though it appears naturally in other colors as well, notably yellow and white. Jade can refer to either of two different silicate minerals: nephrite (a silicate of calcium and magnesium in the amphibole group of minerals), or jadeite (a silicate of sodium and aluminium in the pyroxene group of minerals).
 Leaves of Grass   |  9000000 | 2009 |   985117 |   9.954095 |  9.135970651201838 | Nu Image Films-Grand Army Entertainment-Class 5 Films-Millennium Films-Langley Films-Leaves Productions                                        | https://en.wikipedia.org/wiki/Leaves_of_Grass      | Leaves of Grass is a poetry collection by American poet Walt Whitman (1819–1892), each poem of which is loosely connected and represents the celebration of his philosophy of life and humanity. Though it was first published in 1855, Whitman spent most of his professional life writing and rewriting Leaves of Grass,Miller, 57 revising it multiple times until his death.
 Evolution         | 80000000 | 2001 | 98376292 |   5.395127 | 0.8132040593682877 | DreamWorks SKG-Columbia Pictures Corporation-Montecito Picture Company, The                                                                    | https://en.wikipedia.org/wiki/Evolution            | Evolution is change in the heritable characteristics of biological populations over successive generations. These characteristics are the expressions of genes that are passed on from parent to offspring during reproduction.
 Evolution         | 80000000 | 2001 | 98376292 |   5.395127 | 0.8132040593682877 | DreamWorks SKG-Columbia Pictures Corporation-Montecito Picture Company, The                                                                    | https://en.wikipedia.org/wiki/Evolution/Revolution | Evolution/Revolution: The Early Years (1966–1974) is a two-CD compilation of live stand-up comedy recordings by comedian and actor Richard Pryor, that predate his 1974 mainstream breakthrough album That Nigger's Crazy.
 Killers           | 75000000 | 2010 | 98159963 |   6.540621 | 0.7640589677076386 | Katalyst Films-Lionsgate-Aversano Films                                                                                                        | https://en.wikipedia.org/wiki/Killers              | Killers or The Killers may refer to:
 The Thin Red Line | 52000000 | 1998 | 98126565 |   9.783966 |  0.529927853889515 | Fox 2000 Pictures-Phoenix Pictures-Geisler-Roberdeau                                                                                           | https://en.wikipedia.org/wiki/The_Thin_Red_Line    | The Thin Red Line may refer to:
```



