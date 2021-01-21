import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
import pyspark.sql.functions as f
import psycopg2
from smart_open import open as sopen
import boto3

bucket='data-emr-analytics'
prefix='output/top_1000/'

wiki_xml_file='enwiki-latest-abstract.xml'
movies_metadata='movies_metadata.csv'

xml_s3_path="s3n://%s/xml/%s" % (bucket,wiki_xml_file)
csv_s3_path="s3n://%s/csv/%s" % (bucket,movies_metadata)
csv_output_path="s3n://%s/%s" % (bucket,prefix)

postgres_server='data-analytics.cvilmwwj2hq5.eu-central-1.rds.amazonaws.com'
dbname='postgres'
dbuser='postgres'
password='Inventia2020!'

join_sql_query="""
select t1.title,
t1.budget,
t1.year,
t1.revenue,
t1.popularity,
t1.budget/t1.revenue as ratio,
t1.companiesList,
t2.url,
t2.abstract 
from movies_metadata t1 inner join wiki_pages t2 on replace(t1.title,' ','_')=t2.shortUrl 
where round(t1.budget/t1.revenue)>0 
order by 4 desc nulls last 
limit 1000
"""

def get_s3_keys(bucket, prefix='', suffix=''):

    keys=[]

    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    if isinstance(prefix, str):
       kwargs['Prefix'] = prefix

    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
               keys.append(key)

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

def joinDataSet():

	spark = SparkSession.builder.appName('csv_parse').getOrCreate()

	#Load xml
	xml_df = spark.read.format('com.databricks.spark.xml'). \
	option("rootTag", "feed").option("rowTag","doc"). \
	load(xml_s3_path). \
	withColumn("shortUrl",f.split(f.col("url"),"/").getItem(4))

	selectedData = xml_df.select("url","abstract","shortUrl")
	
	selectedData.show(20,False)
	selectedData.createOrReplaceTempView("wiki_pages")

	#Load csv
	json_schema = ArrayType(StructType([StructField('name', StringType(), nullable=False), StructField('id', IntegerType(), nullable=False)]))

	df = spark.read.option("header",True).csv(csv_s3_path). \
	withColumn("year",f.split(f.col("release_date"),"-").getItem(0)). \
	withColumn("companiesList",f.from_json(f.col("production_companies"),json_schema)). \
	withColumn("companiesList",f.concat_ws("|",f.col("companiesList.name")))

	df.show(20,False)
	df.createOrReplaceTempView("movies_metadata")

	# Join datasets
	q=spark.sql(join_sql_query)
	q.show(20,False)

	# Write output to s3
	q.repartition(1).write.option("sep","\t").format('csv').save(csv_output_path,header = 'false')
	



#######################
###  POSTGRES       ###
#######################

def loadPostgres():

	# Load to postgres

	csv_files=get_s3_keys(bucket,prefix,'csv')
	csv_path="s3://%s/%s" %(bucket,csv_files[0])
	print(csv_path)

	conn_string="host=%s dbname=%s user=%s password=%s" % (postgres_server,dbname,dbuser,password)
	conn = psycopg2.connect(conn_string)
	cur = conn.cursor()

	cur.execute("""DROP TABLE top1000""")
	conn.commit()
	cur.execute("""CREATE TABLE top1000(
				title text,
				budget float8,
				year integer,
				revenue float8,
				vote_average float8,
				ratio float8,
				companiesList text,
				url text,
				abstract text
	)
	""")
	conn.commit()


	with sopen(csv_path, 'r') as f:
		 cur.copy_from(f, 'top1000', sep='\t')

	conn.commit()


def main():
 
    joinDataSet()
    loadPostgres()

if __name__=='__main__':
  main()
