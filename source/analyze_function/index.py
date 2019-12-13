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
from datetime import datetime
import logging

from aws_xray_sdk.core import patch
patch(('boto3',))

comprehend_entity_score_threshold = float(os.environ['COMPREHEND_ENTITY_SCORE_THRESHOLD'])
indexing_stream = os.environ['INDEXING_STREAM']

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
    text_without_url = re.sub(r'https?://[\w/:%#\$&\?\(\)~\.=\+\-]+', '', text_without_account)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    #tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    #text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_without_emoji.splitlines())
    return text_replaced_indention

def gen_index(prefix, dtime):
    return prefix + dtime.strftime('%Y-%m-%d')

def lambda_handler(event, context):
    es_records = []
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string)

        if 'retweeted_status' in tweet:
            is_retweeted = True
            tweet = tweet['retweeted_status']
            #continue
        else:
            is_retweeted = False
        if tweet.get('truncated', False):
            text = tweet['extended_tweet']['full_text']
        else:
            text = tweet['text']
        created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
        normalized_text = normalize(text)
        es_record = {
            'op_type': 'update',
            '_index': gen_index('tweets-', created_at),
            '_id': tweet['id_str'],
            #'tweetid': tweet['id_str'],
            'text': text,
            'normalized_text': normalized_text,
            'lang': tweet['lang'],
            'created_at': created_at.strftime('%s'),
            'url': 'https://twitter.com/{0}/status/{1}'.format(tweet['user']['screen_name'], tweet['id_str']),
            'user': {},
        }
        for attribute in ['filter_level', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count']:
            if attribute in tweet:
                es_record[attribute] = tweet[attribute]
        if 'source' in tweet:
            es_record['source'] = BeautifulSoup(tweet['source'], 'html.parser').getText()
        if 'coordinates' in tweet and tweet['coordinates']:  # None が入ってることがある
            es_record['coordinates'] = tweet['coordinates']['coordinates']
        #if 'hashtags' in tweet['entities']:
        hashtags = [h['text'].lower() for h in tweet['entities'].get('hashtags', [])]
        if len(hashtags) > 0:
            es_record['hashtags'] = hashtags
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        for attribute in ['id_str', 'name', 'screen_name','followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count', 'lang']:
            if attribute in tweet.get('user', {}):
                es_record['user'][attribute] = tweet['user'][attribute]

        if not is_retweeted:
            # retweet の時は数値の更新のみ。
            subsegment = xray_recorder.begin_subsegment('inspect-tweet-text')
            subsegment.put_annotation('_id', tweet['id_str'])

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

            sentiment_response = comprehend.detect_sentiment(
                Text=comprehend_text,
                LanguageCode=comprehend_lang
            )
            #print(sentiment_response)

            entities = []
            entities_response = comprehend.detect_entities(
                Text=comprehend_text,
                LanguageCode=comprehend_lang
            )
            #print(entities_response)
            # 分液用に Elasticsearch に送っておく
            d = datetime.strptime(entities_response['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S %Z')
            for entity in entities_response['Entities']:
                es_records.append({
                    'PartitionKey': entities_response['ResponseMetadata']['RequestId'],
                    'Data': json.dumps({
                        'op_type': 'index',
                        '_index': gen_index('comprehend-entities-', d),
                        'score': entity['Score'],
                        'type': entity['Type'],
                        'text': entity['Text'],
                        'input': {
                            'text': comprehend_text,
                            'lang': comprehend_lang,
                        },
                        'metadata': {
                            'request_id': entities_response['ResponseMetadata']['RequestId'],
                            'date': d.strftime('%s'),
                        }
                    }) + '\n',
                })
                if entity['Type'] in ['QUANTITY', 'DATE']:
                    continue
                elif len(entity['Text']) < 2:
                    continue
                elif entity['Score'] >= comprehend_entity_score_threshold:
                    entities.append(entity['Text'].lower())

            key_phrases = []
            #key_phrases_response = comprehend.detect_key_phrases(
            #    Text=comprehend_text,
            #    LanguageCode=comprehend_lang
            #)
            #for key_phrase in key_phrases_response['KeyPhrases']:
            #    key_phrases.append(key_phrase['Text'])
            #print(key_phrases_response)

            es_record['comprehend'] = { 
                'text': comprehend_text,    
                'lang': comprehend_lang,    
                'sentiment': sentiment_response['Sentiment'],
                'sentiment_score': {
                    'positive': sentiment_response['SentimentScore']['Positive'],
                    'negative': sentiment_response['SentimentScore']['Negative'],
                    'neutral': sentiment_response['SentimentScore']['Neutral'],
                    'mixed': sentiment_response['SentimentScore']['Mixed'],
                }
            }
            if len(entities) > 0:
                es_record['comprehend']['entities'] = list(set(entities))
            if len(key_phrases) > 0:
                es_record['comprehend']['key_phrases'] = list(set(key_phrases))
            xray_recorder.end_subsegment()

        es_records.append({
            'Data': json.dumps(es_record) + '\n',
            'PartitionKey': es_record['_id']
        })

    if len(es_records) > 0:
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        logger.info(res)
    return 'true'