# -*- coding: utf-8 -*-
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
import logging
import hashlib

from aws_xray_sdk.core import patch
patch(('boto3',))

comprehend_entity_score_threshold = float(os.getenv('COMPREHEND_ENTITY_SCORE_THRESHOLD'))
indexing_stream = os.getenv('INDEXING_STREAM')

config = Config(
    retries=dict(max_attempts=20)
)
comprehend = boto3.client('comprehend', config=config)
translate = boto3.client('translate', config=config)
kinesis = boto3.client('kinesis')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def normalize(text):
    text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text_without_account)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    #tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    #text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_without_emoji.splitlines())
    return text_replaced_indention.lower()

def gen_es_record(tweet, update_user_metrics=True, enable_comprehend=True):
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
        #'is_retweet': is_retweet,
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
        for att in ['id_str', 'name', 'screen_name', 'lang']:
            if att in tweet['user']:
                es_record['user'][att] = tweet['user'][att]
        if update_user_metrics: # retweet の場合など、過去のフォロワー数等を上書きしたくない場合はスキップする
            for att in ['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']:
                if att in tweet['user']:
                    es_record['user'][att] = tweet['user'][att]
    elif 'username' in tweet:  # クロールしたデータ用
        es_record['username'] = tweet['username']
    es_record['url'] = f'https://twitter.com/{es_record["username"]}/status/{es_record["id_str"]}'
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
        entities = []
        for entity in res_entities['Entities']:
            if entity['Type'] in ['QUANTITY', 'DATE']:
                continue
            #elif len(entity['Text']) < 2:  # 1文字のとき
            #    continue
            elif entity['Score'] >= comprehend_entity_score_threshold:
                entities.append(entity['Text'].lower())
        if len(entities) > 0:
            es_record['comprehend']['entities'] = list(set(entities))
    return es_record

def lambda_handler(event, context):
    es_records = []
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string)

        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        time_threshold = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp())
        if created_at < time_threshold:
            continue  # 365日以上前の場合はスキップ

        es_record = gen_es_record(tweet)

        if es_record['is_retweet_status']:
            org_es_record = gen_es_record(tweet['retweeted_status'], update_user_metrics=False, enable_comprehend=False)
        elif es_record['is_quote_status']:
            org_es_record = gen_es_record(tweet['quoted_status'], update_user_metrics=False, enable_comprehend=False)
        else:
            org_es_record = None

        if org_es_record and org_es_record['created_at'] >= time_threshold:
            es_records.append({
                'Data': json.dumps(org_es_record) + '\n',
                'PartitionKey': org_es_record['id_str']
            })

        es_records.append({
            'Data': json.dumps(es_record) + '\n',
            'PartitionKey': es_record['id_str']
        })

        if len(es_records) >= 100:
            res = kinesis.put_records(
                Records=es_records,
                StreamName=indexing_stream
            )
            es_records = []  # 初期化
            logger.info(res)

    if len(es_records) > 0:
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        logger.info(res)
    return 'true'