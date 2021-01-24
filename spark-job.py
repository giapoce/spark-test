import psycopg2
import boto3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
import pyspark.sql.functions as f
from smart_open import open as sopen

bucket='data-emr-analytics'
prefix='output/top_1000/'
prefix2='output/wiki/'
prefix3='output/movies/'

wiki_xml_file='enwiki-latest-abstract.xml'
movies_metadata='movies_metadata.csv'

xml_s3_path="s3n://%s/xml/%s" % (bucket,wiki_xml_file)
csv_s3_path="s3n://%s/csv/%s" % (bucket,movies_metadata)
csv_output_path="s3n://%s/%s" % (bucket,prefix)
csv_output_path_2="s3n://%s/%s" % (bucket,prefix2)
csv_output_path_3="s3n://%s/%s" % (bucket,prefix3)

postgres_server='data-emr-analytics.cvilmwwj2hq5.eu-central-1.rds.amazonaws.com'
dbname='postgres'
dbuser='postgres'
password='XXXXX'

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
from movies_metadata t1 inner join wiki_pages t2 
on t1.title=t2.title
and t1.sanitizedTitle=t2.shortUrl
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


#######################
###  SPARK          ###
#######################

def joinDataSet():

	spark = SparkSession.builder.appName('csv_parse').getOrCreate()

	#Load xml
	xml_df = spark.read.format('com.databricks.spark.xml'). \
	option("rootTag", "feed"). \
        option("rowTag","doc"). \
	load(xml_s3_path). \
        withColumn("title",f.ltrim(f.split(f.col("title"),":").getItem(1))). \
	withColumn("shortUrl",f.split(f.col("url"),"/"))

	selectedData = xml_df.select("title","url",f.element_at(f.col('shortUrl'), -1).alias('shortUrl'),"abstract")
	selectedData.repartition(1).write.option("sep","\t").format('csv').mode("overwrite").save(csv_output_path_2,header = 'false')
	selectedData.createOrReplaceTempView("wiki_pages")

	#Load csv
	json_schema = ArrayType(StructType([StructField('name', StringType(), nullable=False), StructField('id', IntegerType(), nullable=False)]))

	df = spark.read.option("header",True). \
	option("quote","\""). \
	option("escape","\""). \
	option("multiLine",True). \
	csv(csv_s3_path). \
	withColumn("sanitizedTitle",f.regexp_replace(f.col("title"),"\\s+","_")). \
	withColumn("year",f.split(f.col("release_date"),"-").getItem(0)). \
	withColumn("companiesList",f.from_json(f.col("production_companies"),json_schema)). \
	withColumn("companiesList",f.concat_ws("-",f.col("companiesList.name")))

	csvSelectedData=df.select("title","sanitizedTitle")
	csvSelectedData.repartition(1).write.option("sep","\t").format('csv').mode("overwrite").save(csv_output_path_3,header = 'false')

	df.createOrReplaceTempView("movies_metadata")

	# Join datasets
	q=spark.sql(join_sql_query)

	# Write output to s3
	q.repartition(1).write.option("sep","\t").format('csv').mode("overwrite").save(csv_output_path,header = 'false')
	



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

	cur.execute("""CREATE TABLE IF NOT EXISTS top1000(
				title text,
				budget float8,
				year integer,
				revenue float8,
				popularity float8,
				ratio float8,
				companiesList text,
				url text,
				abstract text
	)
	""")
	cur.execute("""DELETE FROM top1000""")
	conn.commit()


	with sopen(csv_path, 'r') as f:
		 cur.copy_from(f, 'top1000', sep='\t')

	conn.commit()


def main():
 
    joinDataSet()
    loadPostgres()


if __name__=='__main__':
  main()
