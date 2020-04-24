# -*- coding: utf-8 -*-
import json
import boto3
import os
import base64
import logging
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

from aws_xray_sdk.core import patch
patch(('httplib',))

es_host = os.getenv('ELASTICSEARCH_HOST')
region = os.getenv('AWS_REGION')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def gen_index(prefix, dtime):
    return prefix + dtime.strftime('%Y-%m')

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

def lambda_handler(event, context):
    bulk_data = ''
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        record_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        record_dict = json.loads(record_string)

        if 'timestamp_ms' in record_dict and 'id_str' in record_dict:
            dtime = datetime.fromtimestamp(int(record_dict['timestamp_ms']) / 1000, timezone.utc)
            bulk_header = {
                'update': {
                    '_index': gen_index('tweets-', dtime),
                    '_id': record_dict.pop('id_str')
                }
            }
            bulk_data += json.dumps(bulk_header) + '\n'
            bulk_data += json.dumps({'doc': record_dict, 'doc_as_upsert': True}) + '\n'

    if len(bulk_data) > 0:
        res = es_bulk_load(bulk_data)
        logger.info(res)
        if res['errors']:
            logger.error(res['errors'])
            return 'false' 
    return 'true'