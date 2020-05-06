# -*- coding: utf-8 -*-
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from datetime import datetime
from awsglue import DynamicFrame

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
## @args: [database = src_db_name, table_name = src_table_name, additionalOptions = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"}, stream_type = kinesis]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_data_frame.from_catalog(
    database = src_db_name,
    table_name = src_table_name,
    transformation_ctx = "datasource0",
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

## @type: DataSink
## @args: [mapping = [("created_at", "string", "created_at", "string"), ("id", "long", "id", "long"), ("id_str", "string", "id_str", "string"), ("text", "string", "text", "string"), ("source", "string", "source", "string"), ("truncated", "boolean", "truncated", "boolean"), ("in_reply_to_status_id", "long", "in_reply_to_status_id", "long"), ("in_reply_to_status_id_str", "string", "in_reply_to_status_id_str", "string"), ("in_reply_to_user_id", "long", "in_reply_to_user_id", "long"), ("in_reply_to_user_id_str", "string", "in_reply_to_user_id_str", "string"), ("in_reply_to_screen_name", "string", "in_reply_to_screen_name", "string"), ("user", "struct", "user", "struct"), ("coordinates", "struct", "coordinates", "struct"), ("place", "struct", "place", "struct"), ("quoted_status_id", "long", "quoted_status_id", "long"), ("quoted_status_id_str", "string", "quoted_status_id_str", "string"), ("is_quote_status", "boolean", "is_quote_status", "boolean"), ("quoted_status", "string", "quoted_status", "string"), ("retweeted_status", "string", "retweeted_status", "string"), ("quote_count", "long", "quote_count", "long"), ("reply_count", "long", "reply_count", "long"), ("retweet_count", "long", "retweet_count", "long"), ("favorite_count", "long", "favorite_count", "long"), ("entities", "struct", "entities", "struct"), ("favorited", "boolean", "favorited", "boolean"), ("retweeted", "boolean", "retweeted", "boolean"), ("possibly_sensitive", "boolean", "possibly_sensitive", "boolean"), ("filter_level", "string", "filter_level", "string"), ("lang", "string", "lang", "string"), ("timestamp_ms", "timestamp", "timestamp_ms", "timestamp")], stream_batch_time = "100 seconds", stream_checkpoint_location = "s3://social-media-dashboard-tweetsbucket-1udrh16vozfey/archive/checkpoint/", connection_type = "s3", path = "s3://social-media-dashboard-tweetsbucket-1udrh16vozfey/archive", format = "parquet", transformation_ctx = "datasink1"]
## @return: datasink1
## @inputs: [frame = datasource0]
def add_partition_column(rec):
    timestamp = int(rec['timestamp_ms']) / 1000
    dtime = datetime.utcfromtimestamp(timestamp)
    rec['year'] = str(dtime.year)
    rec['month'] = str(dtime.month)
    rec['day'] = str(dtime.day)
    return rec

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        distinct_data_frame = data_frame.distinct().coalesce(1)
        dynamic_frame = DynamicFrame.fromDF(distinct_data_frame, glueContext, "from_data_frame")
        mapped_dyF =  Map.apply(
            frame = dynamic_frame,
            f = add_partition_column,
            transformation_ctx = "add_partition_column")
        apply_mapping = ApplyMapping.apply(
            frame = mapped_dyF,
            mappings = [
                ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string"),
                ("created_at", "string", "created_at", "string"), ("id", "long", "id", "long"), ("id_str", "string", "id_str", "string"), ("text", "string", "text", "string"), ("source", "string", "source", "string"), ("truncated", "boolean", "truncated", "boolean"), ("in_reply_to_status_id", "long", "in_reply_to_status_id", "long"), ("in_reply_to_status_id_str", "string", "in_reply_to_status_id_str", "string"), ("in_reply_to_user_id", "long", "in_reply_to_user_id", "long"), ("in_reply_to_user_id_str", "string", "in_reply_to_user_id_str", "string"), ("in_reply_to_screen_name", "string", "in_reply_to_screen_name", "string"), ("user", "struct", "user", "struct"), ("coordinates", "struct", "coordinates", "struct"), ("place", "struct", "place", "struct"), ("quoted_status_id", "long", "quoted_status_id", "long"), ("quoted_status_id_str", "string", "quoted_status_id_str", "string"), ("is_quote_status", "boolean", "is_quote_status", "boolean"), ("quoted_status", "string", "quoted_status", "string"), ("retweeted_status", "string", "retweeted_status", "string"), ("quote_count", "long", "quote_count", "long"), ("reply_count", "long", "reply_count", "long"), ("retweet_count", "long", "retweet_count", "long"), ("favorite_count", "long", "favorite_count", "long"), ("entities", "struct", "entities", "struct"), ("favorited", "boolean", "favorited", "boolean"), ("retweeted", "boolean", "retweeted", "boolean"), ("possibly_sensitive", "boolean", "possibly_sensitive", "boolean"), ("filter_level", "string", "filter_level", "string"), ("lang", "string", "lang", "string"), ("timestamp_ms", "timestamp", "timestamp_ms", "timestamp")],
            transformation_ctx = "apply_mapping")
        datasink1 = glueContext.write_dynamic_frame.from_options(
            frame = apply_mapping,
            connection_type = "s3",
            connection_options = {
                "path": dest_s3_path,
                "partitionKeys": ["year", "month", "day"],
                'compression': 'gzip'
            },
            format = "json",
            transformation_ctx = "datasink1")

glueContext.forEachBatch(
    frame = datasource0,
    batch_function = processBatch,
    options = {
        "windowSize": "600 seconds",
        "checkpointLocation": dest_s3_path + "/checkpoint"})
job.commit()
