import json
import boto3
import os
import base64

from aws_xray_sdk.core import patch
patch(('httplib',))

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

es_host = os.environ['ELASTICSEARCH_HOST']
region = os.environ['AWS_REGION']

def gen_index_name(timestamp_ms):
    timestamp = int(timestamp_ms) / 1000
    ymd = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    index_name = 'tweets-' + ymd
    return index_name

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
        tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string)

        es_index = gen_index_name(tweet['timestamp'])
        bulk_data += json.dumps({"index": {"_index": es_index, "_type": "_doc", "_id": tweet['tweetid']}}) + '\n'
        bulk_data += json.dumps(tweet) + '\n'

    if len(bulk_data) > 0:
        r = es_bulk_load(bulk_data)
        print(r)
    return 'true'