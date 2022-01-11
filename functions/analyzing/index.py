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
import emoji
import re
from datetime import datetime, timedelta, timezone

logger = Logger()
tracer = Tracer()
metrics = Metrics()

tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))
dst_stream = os.getenv('DST_STREAM')

boto_config = Config(
    retries=dict(max_attempts=20)
)
comprehend = boto3.client('comprehend', config=boto_config)
translate = boto3.client('translate', config=boto_config)
kinesis = boto3.client('kinesis')

def normalize(text):
    text_without_rt = text.lstrip('RT ')
    #text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text_without_rt)  # remove URL
    #text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_normalized = text_without_url.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    #tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    #text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_without_emoji.splitlines())
    return text_replaced_indention

def gen_comprehend_object(text, lang):
    if lang not in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']:
        res = translate.translate_text(Text=text, SourceLanguageCode=lang, TargetLanguageCode='en')
        text = res['TranslatedText']
        lang = 'en'
    # Sentiment
    sentiment = comprehend.detect_sentiment(Text=text, LanguageCode=lang)
    # Entities
    entities = comprehend.detect_entities(Text=text, LanguageCode=lang)['Entities']
    data = { 
        'sentiment': sentiment['Sentiment'],
        'sentiment_score': {
            'positive': sentiment['SentimentScore']['Positive'],
            'negative': sentiment['SentimentScore']['Negative'],
            'neutral': sentiment['SentimentScore']['Neutral'],
            'mixed': sentiment['SentimentScore']['Mixed'],
        },
        'entities': [{'score': e['Score'], 'type': e['Type'], 'text': e['Text']} for e in entities],
    }
    return data

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
        output_record = tweet
        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        if created_at < time_threshold:
            continue  # 一定以上前の場合はスキップ
        if 'retweeted_status' in tweet and tweet['retweeted_status'].get('truncated', False):
            output_record['text'] =  'RT ' + tweet['retweeted_status']['extended_tweet']['full_text']
        elif tweet.get('truncated', False):
            output_record['text'] = tweet['extended_tweet']['full_text']
        else:
            output_record['text'] = tweet['text']
        output_record['normalized_text'] = normalize(output_record['text'])
        output_record['comprehend'] = gen_comprehend_object(output_record['normalized_text'], tweet['lang'])
        output_records.append({
            'Data': json.dumps(output_record) + '\n',
            'PartitionKey': output_record['id_str']
        })
    metrics.add_metric(name="OutgoingRecords", unit=MetricUnit.Count, value=len(output_records))
    if len(output_records) > 0:
        res = kinesis.put_records(
            Records=output_records,
            StreamName=dst_stream
        )
    return 'true'
