from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')

try:
    region = os.environ['AWS_REGION']
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
except Exception as e:
    helper.init_failure(e)

def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
@helper.update
def create(event, context):
    host = event['ResourceProperties']['Host']
    name = event['ResourceProperties']['Name']
    body = json.loads(event['ResourceProperties']['Body'])
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    response = IndicesClient(es).put_template(
        name=name,
        body=body,
    )
    logger.info(response)

@helper.delete
def delete(event, context):
    host = event['ResourceProperties']['Host']
    name = event['ResourceProperties']['Name']
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    response = IndicesClient(es).delete_template(
        name=name,
    )
    logger.info(response)
