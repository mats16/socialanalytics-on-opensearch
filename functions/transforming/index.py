# -*- coding: utf-8 -*-
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit
import base64
import json
import os
import boto3
from botocore.config import Config
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

logger = Logger()
tracer = Tracer()
metrics = Metrics()

tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))
#twitter_topics = set(map(lambda x: x.lower(), os.getenv('TWITTER_TOPICS').split(',')))
twitter_langs = os.getenv('TWITTER_LANGS').split(',')
comprehend_entity_score_threshold = float(os.getenv('COMPREHEND_ENTITY_SCORE_THRESHOLD'))
dst_stream = os.getenv('DST_STREAM')

boto_config = Config(
    retries=dict(max_attempts=20)
)
kinesis = boto3.client('kinesis')

def filter_entity(entity):
    if entity['type'] in ['QUANTITY', 'DATE']:
        return False
    elif len(entity['text']) < 2:  # 1文字のとき
        return False
    elif entity['text'].startswith('@'):  # アカウント名の時
        return False
    elif entity['score'] < comprehend_entity_score_threshold:
        return False
    else:
        return True

def transform_record(tweet, update_user_metrics=True):
    is_quote_status = tweet.get('is_quote_status', False)
    is_retweet_status = True if 'retweeted_status' in tweet else False
    transformed_record = {
        'created_at': int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp()),
        'is_retweet_status': is_retweet_status,
        'is_quote_status': is_quote_status,
    }
    for att in [
        'id_str',
        'text',
        'normalized_text',
        'lang',
        'filter_level',
        'quote_count',
        'reply_count',
        'retweet_count',
        'favorite_count',
    ]:
        if att in tweet:
            transformed_record[att] = tweet[att]
    if 'source' in tweet:
        transformed_record['source'] = BeautifulSoup(tweet['source'], 'html.parser').getText()
    if tweet.get('coordinates', None):  # None が入ってることがある
        transformed_record['coordinates'] = tweet['coordinates']['coordinates']
    hashtags = [h['text'].lower() for h in tweet['entities'].get('hashtags', [])]
    if len(hashtags) > 0:
        transformed_record['hashtags'] = hashtags
    if 'user' in tweet:
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        user = {}
        for att in ['id_str', 'name', 'screen_name', 'lang']:
            if att in tweet['user']:
                user[att] = tweet['user'][att]
        if update_user_metrics: # retweet の場合など、過去のフォロワー数等を上書きしたくない場合はスキップする
            for att in ['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']:
                if att in tweet['user']:
                    user[att] = tweet['user'][att]
        transformed_record['user'] = user
        transformed_record['url'] = f'https://twitter.com/{tweet["user"]["screen_name"]}/status/{tweet["id_str"]}'
    if 'comprehend' in tweet:
        comprehend = tweet['comprehend'].copy()
        entities = comprehend.pop('entities')
        transformed_record['comprehend'] = comprehend
        transformed_record['comprehend']['entities'] = list(set([entity['text'].lower() for entity in entities if filter_entity(entity)]))
    return transformed_record

def add_es_fields(record, unixtime_field='created_at', id_field='id_str'):
    unixtime = record[unixtime_field]
    record['_index'] = 'tweets-' + datetime.fromtimestamp(unixtime).strftime('%Y-%m')
    record['_id'] = record[id_field]
    return record

def kinesis_record_to_json(record):
    b64_data = record['kinesis']['data']
    str_data = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
    json_data = json.loads(str_data)
    return json_data

@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    records = event['Records']
    metrics.add_metric(name="IncomingRecords", unit=MetricUnit.Count, value=len(records))
    json_records = list( map(kinesis_record_to_json, records) )
    distinct_json_records = list( { rec['id_str']:rec for rec in json_records }.values() )  # 重複排除
    time_threshold = int((datetime.now(timezone.utc) - timedelta(days=tweet_day_threshold)).timestamp())
    output_records = []
    for tweet in distinct_json_records:
        transformed_record = transform_record(tweet)
        transformed_record['z-raw'] = tweet.copy()
        # todo 古いレコードの更新
        if transformed_record['is_retweet_status'] and 'retweeted_status' in tweet:
            original_tweet = tweet['retweeted_status']
            transformed_original_tweet = transform_record(original_tweet, update_user_metrics=False)
            transformed_record['retweeted_status'] = transformed_original_tweet.copy()

            original_created_at = int(datetime.strptime(original_tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
            if original_tweet['lang'] in twitter_langs and original_created_at >= time_threshold:
                output_records.append({
                    'Data': json.dumps(add_es_fields(transformed_original_tweet)) + '\n',
                    'PartitionKey': transformed_original_tweet['id_str']
                })
            else:
                continue
        elif transformed_record['is_quote_status'] and 'quoted_status' in tweet:
            original_tweet = tweet['quoted_status']
            transformed_original_tweet = transform_record(original_tweet, update_user_metrics=False)
            transformed_record['quoted_status'] = transformed_original_tweet.copy()

            original_created_at = int(datetime.strptime(original_tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
            if original_tweet['lang'] in twitter_langs and original_created_at >= time_threshold:
                output_records.append({
                    'Data': json.dumps(add_es_fields(transformed_original_tweet)) + '\n',
                    'PartitionKey': transformed_original_tweet['id_str']
                })
        output_records.append({
            'Data': json.dumps(add_es_fields(transformed_record)) + '\n',
            'PartitionKey': transformed_record['id_str']
        })
    metrics.add_metric(name="OutgoingRecords", unit=MetricUnit.Count, value=len(output_records))
    if len(output_records) > 0:
        res = kinesis.put_records(
            Records=output_records,
            StreamName=dst_stream
        )
    return 'true'
