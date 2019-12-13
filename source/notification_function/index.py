# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import decimal
import urllib.request
from datetime import datetime
import logging

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(('boto3',))

#table_name = os.environ['DDB_TABLE']
#ddb = boto3.resource('dynamodb')

settings = {
    'key1': {
        'condition': '"firebase" in text',
        'message': '"firebase" in text',
        'protocol': 'chime',
        'endpoint': 'https://hooks.chime.aws/incomingwebhooks/2b73d25d-a640-4103-8fa3-3572d24feb79?token=UmNKWDBpRXJ8MXx0bUdnbnNBUFhXdmlYOE4wbV9Cektaa2dhbnBRRVNIcU1NYlhGT2pyTW04'
    }
}
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string, parse_float=decimal.Decimal)

        if not tweet.get('_index').startswith('tweets-') or tweet.get('is_crawled', False):
            continue

        for key in settings.keys():
            condition = settings[key]['condition']
            message = settings[key]['message']
            protocol = settings[key]['protocol']
            endpoint = settings[key]['endpoint']
            try:
                if eval(condition, {}, tweet):
                    content = f'/md\n[{message}]({tweet["url"]})\n' + json.dumps(tweet, indent=4, ensure_ascii=False)
                else:
                    continue
            except Exception as e:
                msg = str(e)
                logger.warn(f'{msg}: {tweet_string}')
                continue  # リクエスト受ける側が死んじゃうので。。
            if protocol == 'chime':
                payload = {'Content': content}
                headers = {'Content-Type': 'application/json'}
                req = urllib.request.Request(endpoint, json.dumps(payload).encode(), headers)
                with urllib.request.urlopen(req) as res:
                    body = res.read()
                    logger.info(body)
            else:
                logger.warn(f'protocol "{protocol}" is not supported.')
                continue
                    
    return 'true'