import json
import boto3
import os
from bs4 import BeautifulSoup

from aws_xray_sdk.core import patch

patch(['boto3'])

s3 = boto3.resource('s3')
comprehend = boto3.client('comprehend')
translate = boto3.client('translate')
firehose = boto3.client('firehose')

def handler(event, context):
    print(event)
    
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        
        obj = s3.Object(s3_bucket, s3_key)
        tweets_as_string = obj.get()['Body'].read().decode('utf-8') 
        
        tweets = tweets_as_string.split('\n')
        status = {'tweet': 0, 'retweet': 0, 'quote': 0}
        for tweet_string in tweets:
            
            if len(tweet_string) < 1:
                continue
            
            tweet = json.loads(tweet_string)
            if 'retweeted_status' in tweet:
                status['retweet'] += 1
                continue
            elif 'quoted_status' in tweet:
                status['quote'] += 1
            else:
                status['tweet'] += 1
            
            if tweet['lang'] in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']:
                comprehend_text = tweet['text']
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
            
            entities_response = comprehend.detect_entities(
                Text=comprehend_text,
                LanguageCode=comprehend_lang
            )
            #print(entities_response)

            sentiment_record = {
                'tweetid': tweet['id_str'],
                'text': comprehend_text,
                'originaltext': tweet['text'],
                'sentiment': sentiment_response['Sentiment'],
                'sentimentposscore': sentiment_response['SentimentScore']['Positive'],
                'sentimentnegscore': sentiment_response['SentimentScore']['Negative'],
                'sentimentneuscore': sentiment_response['SentimentScore']['Neutral'],
                'sentimentmixedscore': sentiment_response['SentimentScore']['Mixed']
            }
            
            response = firehose.put_record(
                DeliveryStreamName=os.environ['SENTIMENT_STREAM'],
                Record={
                    'Data': json.dumps(sentiment_record) + '\n'
                }
            )
            
            seen_entities = []
            for entity in entities_response['Entities']:
                id = entity['Text'] + '-' + entity['Type']
                if (id in seen_entities) == False:
                    entity_record = {
                        'tweetid': tweet['id_str'],
                        'entity': entity['Text'],
                        'type': entity['Type'],
                        'score': entity['Score']
                    }
                    
                    response = firehose.put_record(
                        DeliveryStreamName=os.environ['ENTITY_STREAM'],
                        Record={
                            'Data': json.dumps(entity_record) + '\n'
                        }
                    )
                    seen_entities.append(id)

            key_phrases_response = comprehend.detect_key_phrases(
                Text=comprehend_text,
                LanguageCode=comprehend_lang
            )
            enriched_record = {
                'tweetid': tweet['id_str'],
                'text': tweet['text'],
                'source': BeautifulSoup(tweet['source']).getText(),
                'filter_level': tweet['filter_level'],
                'timestamp': tweet['timestamp_ms'],
                'lang': tweet['lang'],
                'hashtags': [hashtag['text'] for hashtag in tweet['entities']['hashtags']],
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
                    },
                    'entities': [entity['Text'] for entity in entities_response['Entities']],
                    'key_phrases': [i['Text'] for i in key_phrases_response['KeyPhrases']],
                }
            }
            if 'coordinates' in tweet:
                enriched_record['coordinates'] = tweet['coordinates']
            response = firehose.put_record(
                DeliveryStreamName=os.environ['ANALYTICS_STREAM'],
                Record={
                    'Data': json.dumps(enriched_record) + '\n'
                }
            )
        print(json.dumps(status))

    return 'true'