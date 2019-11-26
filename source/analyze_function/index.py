import json
import boto3
import os
import base64
from bs4 import BeautifulSoup
import neologdn
import re

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(('boto3',))

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from requests_aws4auth import AWS4Auth

comprehend_entity_score_threshold = float(os.environ['COMPREHEND_ENTITY_SCORE_THRESHOLD'])
es_host = os.environ['ELASTICSEARCH_HOST']
region = os.environ['AWS_REGION']

comprehend = boto3.client('comprehend')
translate = boto3.client('translate')

def normalize(text):
    text = re.sub(r'https?(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+\$,%#]+)', '', text)  # remove URL
    #text = re.sub(r'@[a-zA-Z0-9_]+(\s)', '', text)  # remove twitter_account
    text = neologdn.normalize(text)
    return text

def gen_index_name(timestamp_ms):
    timestamp = int(timestamp_ms) / 1000
    ymd = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    index_name = 'tweets-' + ymd
    return index_name

@xray_recorder.capture('elasticsearch')
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
        print(key_phrases_response)

        enriched_record = {
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
            enriched_record['comprehend']['entities'] = list(set(entities))

        if len(key_phrases) > 0:
            enriched_record['comprehend']['key_phrases'] = list(set(key_phrases))

        if 'source' in tweet:
            enriched_record['source'] = BeautifulSoup(tweet['source']).getText()

        if 'filter_level' in tweet:
            enriched_record['filter_level'] = tweet['filter_level']

        if 'hashtags' in tweet['entities']:
            hashtags = [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']]
            if len(hashtags) > 0:
                enriched_record['hashtags'] = hashtags

        if 'coordinates' in tweet and tweet['coordinates']:
            enriched_record['coordinates'] = tweet['coordinates']['coordinates']

        es_index = gen_index_name(enriched_record['timestamp'])
        bulk_data += json.dumps({"index": {"_index": es_index, "_type": "_doc", "_id": enriched_record['tweetid']}}) + '\n'
        bulk_data += json.dumps(enriched_record) + '\n'

        xray_recorder.end_subsegment()
    if len(bulk_data) > 0:
        r = es_bulk_load(bulk_data)
        print(r)
    return 'true'