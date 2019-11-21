import json
import boto3
import os
import base64
from bs4 import BeautifulSoup
import neologdn
import re

from aws_xray_sdk.core import patch
patch(['boto3'])

comprehend_entity_score_threshold = float(os.environ['COMPREHEND_ENTITY_SCORE_THRESHOLD'])
delivery_stream_name = os.environ['ANALYTICS_STREAM']

comprehend = boto3.client('comprehend')
translate = boto3.client('translate')
firehose = boto3.client('firehose')

def normalize(text):
    text = re.sub(r'https?(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+\$,%#]+)', '', text)  # remove URL
    #text = re.sub(r'@[a-zA-Z0-9_]+(\s)', '', text)  # remove twitter_account
    text = neologdn.normalize(text)
    return text

def lambda_handler(event, context):
    print(event)
    
    result = {'tweet': 0, 'retweet': 0, 'quote': 0}
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        tweet_string = base64.b64decode(b64_data).decode('utf-8')
        tweet = json.loads(tweet_string)

        if 'retweeted_status' in tweet:
            result['retweet'] += 1
            continue
        elif 'quoted_status' in tweet:
            result['quote'] += 1
        else:
            result['tweet'] += 1

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

        entities = []
        entities_response = comprehend.detect_entities(
            Text=comprehend_text,
            LanguageCode=comprehend_lang
        )
        print(entities_response)
        for entity in entities_response['Entities']:
            if entity['Type'] in ['QUANTITY', 'DATE']:
                continue
            elif len(entity['Text']) < 2:
                continue
            elif entity['Score'] >= comprehend_entity_score_threshold:
                entities.append(entity['Text'])

        key_phrases = []
        key_phrases_response = comprehend.detect_key_phrases(
            Text=comprehend_text,
            LanguageCode=comprehend_lang
        )
        for key_phrase in key_phrases_response['KeyPhrases']:
            key_phrases.append(key_phrase['Text'])

        enriched_record = {
            'tweetid': tweet['id_str'],
            'text': tweet['text'],
            'source': BeautifulSoup(tweet['source']).getText(),
            'filter_level': tweet['filter_level'],
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

        if 'hashtags' in tweet['entities']:
            hashtags = [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']]
            if len(hashtags) > 0:
                enriched_record['hashtags'] = hashtags

        if 'coordinates' in tweet:
            enriched_record['coordinates'] = tweet['coordinates']

        response = firehose.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': json.dumps(enriched_record) + '\n'
            }
        )
    print(result)
    return 'true'