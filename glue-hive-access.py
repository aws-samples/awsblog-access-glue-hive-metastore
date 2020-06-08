import sys
import boto3, os
import requests
from pyspark.sql import SparkSession
from pyspark.sql import Row
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

def rowExpander(row):
    rowDict = row.asDict()
    valProvince = rowDict.pop('Province/State')
    valCountry = rowDict.pop('Country/Region')
    valLat = rowDict.pop('Lat')
    valLong = rowDict.pop('Long')
    for k in rowDict:
        yield Row(**{'province': valProvince , 'country':valCountry, 'latitude':valLat, 'longitude':valLong, 'date' : k, 'value' : row[k]})

# REPLACE THE VALUES BELOW WITH THE OUTPUT PARAMETERS FROM THE CLOUD FORMATION TEMPLATE BEFORE RUNNING THE GLUE JOB
# Parameter EMRMasterDNSName from Cloud Formation Output
hive_thrift_server_name='EMRMasterDNSName'
#Parameter S3BucketName from Cloud Formation Output
s3_bucket_name='S3BucketName'

# delete the contents from the bucket if present.
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket_name)
bucket.objects.delete() 
#s3.Object(s3_bucket_name, 'output/').delete()
#s3.Object(s3_bucket_name, 'covid_data/').delete()

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Get the data from the public url for downloading to the S3 bucket created by the Cloud Formation Template
url1='https://bit.ly/3d93pa1'
url2 = 'https://bit.ly/2ZH4ja0'
url3 = 'https://bit.ly/36BL7vB'

contents1 = requests.get(url1).content
csv_file1 = open('downloaded_1.csv', 'wb')
csv_file1.write(contents1)
csv_file1.close()

contents2 = requests.get(url2).content
csv_file2 = open('downloaded_2.csv', 'wb')
csv_file2.write(contents2)
csv_file2.close()

contents3 = requests.get(url3).content
csv_file3 = open('downloaded_3.csv', 'wb')
csv_file3.write(contents3)
csv_file3.close()

boto3.Session().resource('s3').Bucket(s3_bucket_name).Object(os.path.join('covid_data/csv/confirmed_cases', 'covid_confirmed_global_data.csv')).upload_file('downloaded_1.csv')

boto3.Session().resource('s3').Bucket(s3_bucket_name).Object(os.path.join('covid_data/csv/death_cases', 'covid_deaths_global_data.csv')).upload_file('downloaded_2.csv')

boto3.Session().resource('s3').Bucket(s3_bucket_name).Object(os.path.join('covid_data/csv/recovered_cases', 'covid_recovered_global_data.csv')).upload_file('downloaded_3.csv')

spark = SparkSession \
        .builder \
        .appName("GluePySparkHiveSQL") \
        .config("hive.metastore.uris", "thrift://"+hive_thrift_server_name+":9083") \
        .enableHiveSupport() \
        .getOrCreate()
        
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
# # Convert the CSV data into Parquet format before creating the tables in Hive for the data         
df0 = glueContext.create_dynamic_frame.from_options("s3", {'paths': ["s3://"+s3_bucket_name+"/covid_data/csv/confirmed_cases/" ]}, format="csv", format_options={'withHeader':True}).toDF()
#Since the data has date as the columns, transpose that to rows so the data is represented
newDF0 = spark.createDataFrame(df0.rdd.flatMap(rowExpander))
newDF0.show(10)

datasource0 = DynamicFrame.fromDF(newDF0,glueContext,"datasource0")
applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("province", "string", "province", "string"),("country", "string", "country", "string"), ("latitude", "string", "latitude", "double"), ("longitude", "string", "longitude", "double"), ("date", "string", "record_date", "string"), ("value", "string", "confirmed", "long")], transformation_ctx = "applymapping0")
resolvechoice0 = ResolveChoice.apply(frame = applymapping0, choice = "make_struct", transformation_ctx = "resolvechoice0")
dropnullfields0 = DropNullFields.apply(frame = resolvechoice0, transformation_ctx = "dropnullfields0")
datasink0 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields0, connection_type = "s3", connection_options = {"path": "s3://"+s3_bucket_name+"/covid_data/parquet/covid_confirmed_cases"}, format = "parquet", transformation_ctx = "datasink0")

df1 = glueContext.create_dynamic_frame.from_options("s3", {'paths': ["s3://"+s3_bucket_name+"/covid_data/csv/death_cases/" ]}, format="csv", format_options={'withHeader':True}).toDF()
#Since the data has date as the columns, transpose that to rows so the data is represented
newDF1 = spark.createDataFrame(df1.rdd.flatMap(rowExpander))
newDF1.show(10)

datasource1 = DynamicFrame.fromDF(newDF1,glueContext,"datasource1")
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("province", "string", "province", "string"),("country", "string", "country", "string"), ("latitude", "string", "latitude", "double"), ("longitude", "string", "longitude", "double"), ("date", "string", "record_date", "string"), ("value", "string", "deaths", "long")], transformation_ctx = "applymapping1")
resolvechoice1 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice1")
dropnullfields1 = DropNullFields.apply(frame = resolvechoice1, transformation_ctx = "dropnullfields1")
datasink1 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields1, connection_type = "s3", connection_options = {"path": "s3://"+s3_bucket_name+"/covid_data/parquet/covid_death_cases"}, format = "parquet", transformation_ctx = "datasink1")

df2 = glueContext.create_dynamic_frame.from_options("s3", {'paths': ["s3://"+s3_bucket_name+"/covid_data/csv/recovered_cases/" ]}, format="csv", format_options={'withHeader':True}).toDF()
#Since the data has date as the columns, transpose that to rows so the data is represented
newDF2 = spark.createDataFrame(df1.rdd.flatMap(rowExpander))
newDF2.show(10)
datasource2 = DynamicFrame.fromDF(newDF2,glueContext,"datasource2")
applymapping2 = ApplyMapping.apply(frame = datasource2, mappings = [("province", "string", "province", "string"),("country", "string", "country", "string"), ("latitude", "string", "latitude", "double"), ("longitude", "string", "longitude", "double"), ("date", "string", "record_date", "string"), ("value", "string", "recovered", "long")], transformation_ctx = "applymapping2")
resolvechoice2 = ResolveChoice.apply(frame = applymapping2, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields2 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields2")
datasink2 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields2, connection_type = "s3", connection_options = {"path": "s3://"+s3_bucket_name+"/covid_data/parquet/covid_recovered_cases"}, format = "parquet", transformation_ctx = "datasink2")

droptable1 = 'DROP TABLE IF EXISTS covid_recovered_cases'
droptable2 = 'DROP TABLE IF EXISTS covid_confirmed_cases'
droptable3 = 'DROP TABLE IF EXISTS covid_death_cases'

spark.sql(droptable1)
spark.sql(droptable2)
spark.sql(droptable3)

# # DDL for Hive Tables using the parquet data 
createtable1 = "CREATE EXTERNAL TABLE covid_recovered_cases(province string, country string, latitude double, longitude double, record_date string, recovered bigint)" \
  " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "\
  " STORED AS INPUTFORMAT " \
  "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " \
  "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " \
  "LOCATION  's3n://" +s3_bucket_name+ "/covid_data/parquet/covid_recovered_cases'"\
  " TBLPROPERTIES ( 'skip.header.line.count'='1')"

createtable2 = "CREATE EXTERNAL TABLE covid_confirmed_cases(province string, country string, latitude double, longitude double, record_date string, confirmed bigint)" \
  " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"\
  " STORED AS INPUTFORMAT " \
  "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " \
  "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " \
  "LOCATION  's3n://" +s3_bucket_name+ "/covid_data/parquet/covid_confirmed_cases'"\
  " TBLPROPERTIES (" \
  "'skip.header.line.count'='1')"

createtable3 = "CREATE EXTERNAL TABLE covid_death_cases( province string, country string, latitude double, longitude double, record_date string, deaths bigint)" \
  " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "\
  " STORED AS INPUTFORMAT " \
  "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " \
  "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " \
  "LOCATION  's3n://" +s3_bucket_name+ "/covid_data/parquet/covid_death_cases'"\
  " TBLPROPERTIES (" \
  "'skip.header.line.count'='1')"

# #  Create the hive tables.

sqlDF1 = spark.sql(createtable1)
sqlDF2 = spark.sql(createtable2)
sqlDF3 = spark.sql(createtable3)


# # Run queries against the input data and store in the output folder in the S3 bucket
sql1DF = spark.sql("select covid_confirmed_cases.country as country , sum(deaths) as total_deaths , sum(confirmed) as total_confirmed, sum(recovered) as total_recovered from covid_confirmed_cases join covid_death_cases on covid_confirmed_cases.country=covid_death_cases.country join covid_recovered_cases on covid_death_cases.country=covid_recovered_cases.country where covid_confirmed_cases.record_date between '5/01/20' and '5/31/20' group by covid_confirmed_cases.country").repartition(1)
sql1DF.write.csv("s3://"+s3_bucket_name+"/output/query1")

sql2DF = spark.sql("select country, sum(deaths) as total_deaths, month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd')) as month from covid_death_cases  group by country, month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd'))").repartition(1)
sql2DF.write.csv("s3://"+s3_bucket_name+"/output/query2")


sql3DF = spark.sql("select country, sum(recovered) as total_recovered, month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd')) as month from covid_recovered_cases  group by country, month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd'))").repartition(1)
sql3DF.write.csv("s3://"+s3_bucket_name+"/output/query3")

sql4DF = spark.sql("select country, sum(confirmed) as total_confirmed , month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd')) as month from covid_confirmed_cases  group by country, month(from_unixtime(unix_timestamp(record_date ,'M/dd/yy'), 'yyyy-MM-dd'))").repartition(1)
sql4DF.write.csv("s3://"+s3_bucket_name+"/output/query4")