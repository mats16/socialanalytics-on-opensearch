# -*- coding: utf-8 -*-
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SRC_DB_NAME', 'SRC_TABLE_NAME', 'DEST_S3_PATH'])
src_db_name = args['SRC_DB_NAME']
src_table_name = args['SRC_TABLE_NAME']
dest_s3_path = args['DEST_S3_PATH']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "tweets", table_name = "raw", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
now = datetime.now()
ignore_this_month = now.strftime("NOT (year = '%Y' AND month = '%m')")

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = src_db_name, 
    table_name = src_table_name, 
    push_down_predicate = ignore_this_month,
    transformation_ctx = "datasource0")

## @type: FilterRetweets
## @args: [f = filter_retweets, transformation_ctx = "filtered_frame"]
## @return: filtered_frame
## @inputs: [frame = datasource0]
def filter_retweets(dynamicRecord):
    retweeted_status = dynamicRecord.get('retweeted_status')
    if retweeted_status:
        return False
    else:
        return True
filtered_frame = Filter.apply(
    frame = datasource0, 
    f = filter_retweets,
    transformation_ctx = "filtered_frame")

## @type: ApplyMapping
## @args: [mapping = [("id_str", "string", "id_str", "string"), ("text", "string", "source", "string"), ("timestamp_ms", "timestamp", "timestamp_ms", "timestamp"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = filtered_frame]
applymapping1 = ApplyMapping.apply(
    frame = filtered_frame, 
    mappings = [
        ("text", "string", "source", "string"),
        ("id_str", "string", "id_str", "string"),
        ("year", "string", "year", "string"), 
        ("month", "string", "month", "string"), 
    ], 
    transformation_ctx = "applymapping1")



## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": dest_s3_path}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]

applymapping1.toDF().repartition("year", "month").write.partitionBy(["year", "month"]).option("maxRecordsPerFile", 30000).mode('append').json(dest_s3_path)
job.commit()
