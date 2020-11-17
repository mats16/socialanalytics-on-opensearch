# -*- coding: utf-8 -*-
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools import single_metric
from aws_lambda_powertools.metrics import MetricUnit
import json
import boto3
import os
import base64
import logging
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

logger = Logger()
tracer = Tracer(patch_modules=['httplib'])
metrics = Metrics()

es_host = os.getenv('ELASTICSEARCH_HOST')
region = os.getenv('AWS_REGION')

def es_bulk_load(data):
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
    es = Elasticsearch(
        hosts = [{'host': es_host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    r = es.bulk(data)
    return r

@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    bulk_data = ''
    records = set(map(lambda x: x['kinesis']['data'], event['Records']))  # 重複排除
    time_threshold = int((datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp())
    tweet_count, retweet_count, quote_count, old_count = 0, 0, 0, 0
    for record in records:
        record_string = base64.b64decode(record).decode('utf-8').rstrip('\n')
        record_dict = json.loads(record_string)

        if '_index' in record_dict and '_id' in record_dict:
            if record_dict['created_at'] > time_threshold and 'comprehend' in record_dict:
                tweet_count += 1
                if record_dict['is_retweet_status']:
                    retweet_count += 1
                if record_dict['is_quote_status']:
                    quote_count += 1
            else:
                old_count += 1

            bulk_header = {
                'update': {
                    '_index': record_dict.pop('_index'),
                    '_id': record_dict.pop('_id'),
                }
            }
            bulk_data += json.dumps(bulk_header) + '\n'
            bulk_data += json.dumps({'doc': record_dict, 'doc_as_upsert': True}) + '\n'

    if len(bulk_data) > 0:
        res = es_bulk_load(bulk_data)
        logger.info(res)
        metrics.add_dimension(name="FunctionName", value=context.function_name)
        metrics.add_metric(name="ProcessedTweetRecords", unit=MetricUnit.Count, value=tweet_count)
        metrics.add_metric(name="ProcessedRetweetRecords", unit=MetricUnit.Count, value=retweet_count)
        metrics.add_metric(name="ProcessedQuoteRecords", unit=MetricUnit.Count, value=quote_count)
        metrics.add_metric(name="ProcessedOldRecords", unit=MetricUnit.Count, value=old_count)

        with single_metric(name="TookTime", unit=MetricUnit.Count, value=res['took'], namespace="Elasticsearch/Custom") as metric:
            metric.add_dimension(name="API", value="Bulk")

        if res['errors']:
            logger.error(res['errors'])
            return 'false' 
    return 'true'