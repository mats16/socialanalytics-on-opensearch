# -*- coding: utf-8 -*-
# https://docs.aws.amazon.com/ja_jp/elasticsearch-service/latest/developerguide/es-request-signing.html#es-request-signing-python
from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3
import os
import requests
from requests_aws4auth import AWS4Auth
from time import sleep

try:
    logger = logging.getLogger(__name__)
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    region = os.environ['AWS_REGION']
    service='es'
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
    path = event['ResourceProperties']['Path']
    body = json.loads(event['ResourceProperties']['Body'])
    url = f'https://{host}/{path}'
    while True:
        res = requests.put(url, auth=awsauth, json=body)
        if res.status_code in [200, 201]:
            logger.info(res.text)
            break
        elif res.status_code == 401:
            logger.warning(res.text)
            sleep(20)
        else:
            logger.error(res.text)
            raise ValueError(res.text)
    physical_resource_id = url
    return physical_resource_id

@helper.delete
def delete(event, context):
    host = event['ResourceProperties']['Host']
    path = event['ResourceProperties']['Path']
    url = f'https://{host}/{path}'
    try:
        res = requests.delete(url, auth=awsauth)
        logger.info(res.text)
    except Exception as e:
        logger.error(e)