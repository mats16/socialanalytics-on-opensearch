import base64
import json
import os
import boto3
from botocore.config import Config
from bs4 import BeautifulSoup
import neologdn
import re
from datetime import datetime

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(('boto3',))

comprehend_entity_score_threshold = float(os.environ['COMPREHEND_ENTITY_SCORE_THRESHOLD'])
indexing_stream = os.environ['INDEXING_STREAM']

config = Config(
    retries=dict(max_attempts=20)
)
comprehend = boto3.client('comprehend', config=config)
translate = boto3.client('translate', config=config)

def normalize(text):
    text = re.sub(r'https?(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+\$,%#]+)', '', text)  # remove URL
    #text = re.sub(r'@[a-zA-Z0-9_]+(\s)', '', text)  # remove twitter_account
    text = neologdn.normalize(text)
    return text

def gen_index(prefix, timestamp_ms):
    timestamp = int(timestamp_ms) / 1000
    ymd = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    return prefix + ymd

def lambda_handler(event, context):
    es_records = []
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        tweet_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string)

        if 'retweeted_status' in tweet:
            continue

        subsegment = xray_recorder.begin_subsegment('analyzing')
        subsegment.put_annotation('tweetid', tweet['id_str'])
        if tweet['lang'] in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']:
            comprehend_text = normalize(tweet['text'])
            comprehend_lang = tweet['lang']
        else:
            response = translate.translate_text(
                Text=tweet['text'],
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
        for entity in entities_response['Entities']:
            if entity['Type'] in ['QUANTITY', 'DATE']:
                continue
            elif len(entity['Text']) < 2:
                continue
            elif entity['Score'] >= comprehend_entity_score_threshold:
                entities.append(entity['Text'].lower())

        key_phrases = []
        key_phrases_response = comprehend.detect_key_phrases(
            Text=comprehend_text,
            LanguageCode=comprehend_lang
        )
        for key_phrase in key_phrases_response['KeyPhrases']:
            key_phrases.append(key_phrase['Text'])
        #print(key_phrases_response)

        es_record = {
            '_index': gen_index('tweets-', tweet['timestamp_ms']),
            '_id': tweet['id_str'],
            'tweetid': tweet['id_str'],
            'text': tweet['text'],
            'timestamp': tweet['timestamp_ms'],
            'lang': tweet['lang'],
            'url': 'https://twitter.com/{0}/status/{1}'.format(tweet['user']['screen_name'], tweet['id_str']),  
            'comprehend': { 
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
        }

        if len(entities) > 0:
            es_record['comprehend']['entities'] = list(set(entities))

        if len(key_phrases) > 0:
            es_record['comprehend']['key_phrases'] = list(set(key_phrases))

        if 'source' in tweet:
            es_record['source'] = BeautifulSoup(tweet['source']).getText()

        if 'filter_level' in tweet:
            es_record['filter_level'] = tweet['filter_level']

        if 'hashtags' in tweet['entities']:
            hashtags = [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']]
            if len(hashtags) > 0:
                es_record['hashtags'] = hashtags

        if 'coordinates' in tweet and tweet['coordinates']:
            es_record['coordinates'] = tweet['coordinates']['coordinates']

        es_records.append({
            'Data': json.dumps(es_record) + '\n',
            'PartitionKey': es_record['tweetid']
        })

        xray_recorder.end_subsegment()

    if len(es_records) > 0:
        kinesis = boto3.client('kinesis')
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        print(res)
    return 'true'