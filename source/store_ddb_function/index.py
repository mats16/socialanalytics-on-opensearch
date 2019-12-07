# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import decimal
from datetime import datetime

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(('boto3',))

ddb_ttl = 60 * 60 * 24  # 1day

table_name = os.environ['DDB_TABLE']
ddb = boto3.resource('dynamodb')

def fix_empty_strings(tweet_dic):
    """空文字列を None に置換する"""
    def fix_media_info(media_dic):
        for k in ['title', 'description']:
            if media_dic.get('additional_media_info', {}).get(k) == '':
                media_dic['additional_media_info'][k] = None
        return media_dic

    for m in tweet_dic.get('entities', {}).get('media', []):
        m = fix_media_info(m)
    for m in tweet_dic.get('extended_entities', {}).get('media', []):
        m = fix_media_info(m)
    for m in tweet_dic.get('extended_tweet', {}).get('entities', {}).get('media', []):
        m = fix_media_info(m)
    for m in tweet_dic.get('extended_tweet', {}).get('extended_entities', {}).get('media', []):
        m = fix_media_info(m)

    for k in [
        'profile_background_image_url',
        'profile_background_image_url_https',
        'profile_image_url',
        'profile_image_url_https',
    ]:
        if tweet_dic.get('user', {}).get(k) == '':
            tweet_dic['user'][k] = None
    return tweet_dic

def lambda_handler(event, context):
    table = ddb.Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=['id_str']) as batch:
        for record in event['Records']:
            b64_data = record['kinesis']['data']
            tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
            tweet = json.loads(tweet_string, parse_float=decimal.Decimal)

            if 'retweeted_status' in tweet:
                is_retweeted = True
                tweet = tweet['retweeted_status']                
            else:
                is_retweeted = False
            created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
            timestamp = int(created_at.timestamp())
            tweet['date'] = created_at.strftime('%Y-%m-%d')  # string
            tweet['timestamp'] = timestamp  # integer
            tweet['ttl'] = timestamp + ddb_ttl  # integer
            if 'timestamp_ms' not in tweet:
                tweet['timestamp_ms'] = str(timestamp * 1000)  # string

            tweet = fix_empty_strings(tweet)
            if 'quoted_status' in tweet:
                tweet['quoted_status'] = fix_empty_strings(tweet['quoted_status'])

            batch.put_item(Item=tweet)
    return 'true'
