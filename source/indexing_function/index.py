# -*- coding: utf-8 -*-
import json
import boto3
import os
import base64
import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

from aws_xray_sdk.core import patch
patch(('httplib',))

es_host = os.environ['ELASTICSEARCH_HOST']
region = os.environ['AWS_REGION']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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

        if 'op_type' in record_dict:
            op_type = record_dict.pop('op_type')
        else:
            op_type = 'index' 

        if '_id' in record_dict:
            bulk_header = {op_type: {'_index': record_dict.pop('_index'), '_id': record_dict.pop('_id')}}
        else:
            bulk_header = {op_type: {'_index': record_dict.pop('_index')}}

        if op_type == 'update':
            bulk_data += json.dumps(bulk_header) + '\n'
            bulk_data += json.dumps({'doc': record_dict, 'doc_as_upsert': True}) + '\n'
        else:
            bulk_data += json.dumps(bulk_header) + '\n'
            bulk_data += json.dumps(record_dict) + '\n'

    if len(bulk_data) > 0:
        res = es_bulk_load(bulk_data)
        logger.info(res)
        if res['errors']:
            logger.error(res['errors'])
            return 'false' 
    return 'true'