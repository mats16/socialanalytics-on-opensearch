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
import neologdn
import emoji
import re
from datetime import datetime, timedelta, timezone

comprehend_logger = Logger(service="comprehend")
logger = Logger()
tracer = Tracer()
metrics = Metrics()

tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))
tweet_langs = os.getenv('TWEET_LANGS').split(',')
comprehend_entity_score_threshold = float(os.getenv('COMPREHEND_ENTITY_SCORE_THRESHOLD'))
indexing_stream = os.getenv('INDEXING_STREAM')

config = Config(
    retries=dict(max_attempts=20)
)
comprehend = boto3.client('comprehend', config=config)
translate = boto3.client('translate', config=config)
kinesis = boto3.client('kinesis')

def normalize(text):
    #text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    #tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    #text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_without_emoji.splitlines())
    return text_replaced_indention

def gen_es_record(transformed_record):
    unixtime = transformed_record['created_at']
    es_record = transformed_record
    es_record['_index'] = 'tweets-' + datetime.fromtimestamp(unixtime).strftime('%Y-%m')
    es_record['_id'] = transformed_record['id_str']
    return es_record

@tracer.capture_method
def transform_record(tweet, update_user_metrics=True, enable_comprehend=True):
    is_retweet_status = True if 'retweeted_status' in tweet else False
    is_quote_status = tweet.get('is_quote_status', False)

    tweet_text = tweet['extended_tweet']['full_text'] if tweet.get('truncated', False) else tweet['text']
    normalized_text = normalize(tweet_text)
    created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
    es_record = {
        'id_str': tweet['id_str'],
        'text': tweet_text,
        'normalized_text': normalized_text,
        'lang': tweet['lang'],
        'created_at': int(created_at.timestamp()),
        'is_retweet_status': is_retweet_status,
        'is_quote_status': is_quote_status,
    }
    for att in ['filter_level', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count']:
        if att in tweet:
            es_record[att] = tweet[att]
    if 'source' in tweet:
        es_record['source'] = BeautifulSoup(tweet['source'], 'html.parser').getText()
    if tweet.get('coordinates', None):  # None が入ってることがある
        es_record['coordinates'] = tweet['coordinates']['coordinates']
    hashtags = [h['text'].lower() for h in tweet['entities'].get('hashtags', [])]
    if len(hashtags) > 0:
        es_record['hashtags'] = hashtags
    if 'user' in tweet:
        es_record['username'] = tweet['user']['screen_name']
        es_record['user'] = {}
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        if 'id_str' in tweet['user']:
            es_record['user']['id_str'] = tweet['user']['id_str']
            es_record['url'] = f'https://twitter.com/{tweet["user"]["id_str"]}/status/{tweet["id_str"]}'
        for att in ['name', 'screen_name', 'lang']:
            if att in tweet['user']:
                es_record['user'][att] = tweet['user'][att]
        if update_user_metrics: # retweet の場合など、過去のフォロワー数等を上書きしたくない場合はスキップする
            for att in ['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']:
                if att in tweet['user']:
                    es_record['user'][att] = tweet['user'][att]
    elif 'username' in tweet:  # クロールしたデータ用
        es_record['username'] = tweet['username']
        es_record['url'] = f'https://twitter.com/{tweet["username"]}/status/{tweet["id_str"]}'
    if enable_comprehend:
        # Translate before Comprehend
        if tweet['lang'] in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']:
            comprehend_text = normalized_text
            comprehend_lang = tweet['lang']
        else:
            response = translate.translate_text(
                Text=text,
                SourceLanguageCode=tweet['lang'],
                TargetLanguageCode='en')
            comprehend_text = response['TranslatedText']
            comprehend_lang = 'en'
        # Sentiment
        res_sentiment = comprehend.detect_sentiment(
            Text=comprehend_text,
            LanguageCode=comprehend_lang
        )
        #comprehend_logger.info(res_sentiment)
        es_record['comprehend'] = { 
            'text': comprehend_text,    
            'lang': comprehend_lang,    
            'sentiment': res_sentiment['Sentiment'],
            'sentiment_score': {
                'positive': res_sentiment['SentimentScore']['Positive'],
                'negative': res_sentiment['SentimentScore']['Negative'],
                'neutral': res_sentiment['SentimentScore']['Neutral'],
                'mixed': res_sentiment['SentimentScore']['Mixed'],
            }
        }
        # Entities
        res_entities = comprehend.detect_entities(
            Text=comprehend_text,
            LanguageCode=comprehend_lang
        )
        #comprehend_logger.info(res_entities)
        entities = []
        for entity in res_entities['Entities']:
            if entity['Type'] in ['QUANTITY', 'DATE']:
                continue
            elif len(entity['Text']) < 2:  # 1文字のとき
                continue
            elif entity['Score'] >= comprehend_entity_score_threshold:
                entities.append(entity['Text'].lower())
        if len(entities) > 0:
            es_record['comprehend']['entities'] = list(set(entities))
    return es_record

def json_loader(record):
    b64_data = record['kinesis']['data']
    str_data = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
    json_data = json.loads(str_data)
    return json_data

@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    records = event['Records']
    metrics.add_metric(name="IncomingRecords", unit=MetricUnit.Count, value=len(records))
    json_records = list( map(json_loader, records) )
    distinct_json_records = list( { rec['id_str']:rec for rec in json_records }.values() )  # 重複排除
    metrics.add_metric(name="DistinctIncomingRecords", unit=MetricUnit.Count, value=len(distinct_json_records))

    time_threshold = int((datetime.now(timezone.utc) - timedelta(days=tweet_day_threshold)).timestamp())
    tweet_count, retweet_count, quote_count, old_count = 0, 0, 0, 0
    es_records = []
    for tweet in distinct_json_records:
        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        if created_at < time_threshold:
            continue  # 365日以上前の場合はスキップ

        transfored_record = transform_record(tweet)
        tweet_count += 1

        if transfored_record['is_retweet_status']:
            if 'retweeted_status' in tweet:
                retweet_count += 1
                transfored_record_org = transform_record(tweet['retweeted_status'], update_user_metrics=False, enable_comprehend=False)
                transfored_record['retweeted_status'] = transfored_record_org
                if transfored_record_org['lang'] not in tweet_langs:
                    transfored_record_org = None  # 元tweetが他言語の場合、取り込まない
                elif transfored_record_org['created_at'] < time_threshold:
                    logger.info({
                        'state': 'skip_record',
                        'reason': 'The tweet is too old.',
                        'tweet_type': 'retweet',
                        'tweet': transfored_record_org
                    })
                    continue  # 元tweetが一定以上古い場合は全てスキップ
        elif transfored_record['is_quote_status']:
            if 'quoted_status' in tweet:
                quote_count += 1
                transfored_record_org = transform_record(tweet['quoted_status'], update_user_metrics=False, enable_comprehend=False)
                transfored_record['quoted_status'] = transfored_record_org
                if transfored_record_org['lang'] not in tweet_langs:
                    logger.info({
                        'state': 'skip_record',
                        'reason': f'The tweet is not in {str(tweet_langs)}.',
                        'tweet_type': 'quote',
                        'tweet': transfored_record_org
                    })
                    transfored_record_org = None
                elif transfored_record_org['created_at'] < time_threshold:
                    logger.info({
                        'state': 'skip_record',
                        'reason': 'The tweet is too old.',
                        'tweet_type': 'quote',
                        'tweet': transfored_record_org
                    })
                    transfored_record_org = None  # 元tweetが一定以上古い場合は引用tweetのみ（過去indexが無限にできるので）
        else:
            transfored_record_org = None

        for i in [transfored_record, transfored_record_org]:
            if i:
                es_record = gen_es_record(i)
                es_records.append({
                    'Data': json.dumps(es_record) + '\n',
                    'PartitionKey': i['_id']
                })

    if len(es_records) > 0:
        metrics.add_metric(name="OutgoingRecords", unit=MetricUnit.Count, value=len(es_records))
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        logger.info({
            'state': 'put_records',
            'record_count': len(es_records),
            'failed_record_count': res['FailedRecordCount'],
            'text': 'Put records to kinesis'
        })
    return 'true'