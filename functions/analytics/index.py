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

logger = Logger()
tracer = Tracer()
metrics = Metrics()

tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))
twitter_topics = set(map(lambda x: x.lower(), os.getenv('TWITTER_TOPICS').split(',')))
twitter_langs = os.getenv('TWITTER_LANGS').split(',')
comprehend_entity_score_threshold = float(os.getenv('COMPREHEND_ENTITY_SCORE_THRESHOLD'))
indexing_stream = os.getenv('INDEXING_STREAM')

boto_config = Config(
    retries=dict(max_attempts=20)
)
comprehend = boto3.client('comprehend', config=boto_config)
translate = boto3.client('translate', config=boto_config)
kinesis = boto3.client('kinesis')

def normalize(text):
    #text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text)  # remove URL
    #text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_normalized = text_without_url.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
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

def filter_entity(entity):
    if entity['type'] in ['QUANTITY', 'DATE']:
        return False
    elif len(entity['text']) < 2:  # 1文字のとき
        return False
    elif entity['score'] < comprehend_entity_score_threshold:
        return False
    else:
        return True

def gen_comprehend_object(text, lang):
    if lang not in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']:
        res_t = translate.translate_text(Text=text, SourceLanguageCode=lang, TargetLanguageCode='en')
        text = res_t['TranslatedText']
        lang = 'en'
    # Sentiment
    res_s = comprehend.detect_sentiment(Text=text, LanguageCode=lang)
    sentiment = res_s['Sentiment']
    sentiment_score = {
        'positive': res_s['SentimentScore']['Positive'],
        'negative': res_s['SentimentScore']['Negative'],
        'neutral': res_s['SentimentScore']['Neutral'],
        'mixed': res_s['SentimentScore']['Mixed'],
    }
    # Entities
    res_e = comprehend.detect_entities(Text=text, LanguageCode=lang)
    raw_entities = [{'text': e['Text'], 'type': e['Type'], 'score': e['Score']} for e in res_e['Entities']]
    entities = list(set([entity['text'].lower() for entity in raw_entities if filter_entity(entity)]))
    all_entities_text = set(map(lambda e: e['text'].lstrip('#').lower(), raw_entities))
    entities_in_topics = True if len(all_entities_text & twitter_topics) > 0 else False
    data = { 
        'text': text,
        'lang': lang,
        'sentiment': sentiment,
        'sentiment_score': sentiment_score,
        'entities_in_topics': entities_in_topics
    }
    if len(entities) > 0:
        data['entities'] = entities
    if len(raw_entities) > 0:
        data['raw_entities'] = raw_entities
    return data

@tracer.capture_method
def transform_record(tweet, update_user_metrics=True, enable_comprehend=True):
    is_quote_status = tweet.get('is_quote_status', False)
    is_retweet_status = True if 'retweeted_status' in tweet else False
    if is_retweet_status:
        tweet_text =  'RT ' + tweet['retweeted_status']['extended_tweet']['full_text'] if tweet['retweeted_status'].get('truncated', False) else tweet['text']
    else:
        tweet_text = tweet['extended_tweet']['full_text'] if tweet.get('truncated', False) else tweet['text']
    normalized_text = normalize(tweet_text)
    created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
    es_record = {
        'id_str': tweet['id_str'],
        'text': tweet_text,
        'normalized_text': normalized_text,
        'lang': tweet['lang'],
        'created_at': created_at,
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
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        es_record['user'] = {}
        for att in ['id_str', 'name', 'screen_name', 'lang']:
            if att in tweet['user']:
                es_record['user'][att] = tweet['user'][att]
        if update_user_metrics: # retweet の場合など、過去のフォロワー数等を上書きしたくない場合はスキップする
            for att in ['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']:
                if att in tweet['user']:
                    es_record['user'][att] = tweet['user'][att]
        es_record['url'] = f'https://twitter.com/{es_record["user"]["screen_name"]}/status/{es_record["id_str"]}'
        es_record['username'] = es_record['user']['screen_name']
    elif 'username' in tweet:  # クロールしたデータ用
        es_record['username'] = tweet['username']
        es_record['url'] = f'https://twitter.com/{tweet["username"]}/status/{tweet["id_str"]}'
    if enable_comprehend:
        es_record['comprehend'] = gen_comprehend_object(normalized_text, tweet['lang'])
    return es_record

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
    #metrics.add_metric(name="DistinctIncomingRecords", unit=MetricUnit.Count, value=len(distinct_json_records))

    time_threshold = int((datetime.now(timezone.utc) - timedelta(days=tweet_day_threshold)).timestamp())
    tweet_count, retweet_count, quote_count, old_count = 0, 0, 0, 0
    es_records = []
    for tweet in distinct_json_records:
        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        if created_at < time_threshold:
            continue  # 一定以上前の場合はスキップ
        else:
            tweet_count += 1
        transfored_record = transform_record(tweet)
        if transfored_record['is_retweet_status']:
            if 'retweeted_status' in tweet:
                retweet_count += 1
                old_tweet = tweet['retweeted_status']
                old_created_at = int(datetime.strptime(old_tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
                if old_tweet['lang'] in twitter_langs and old_created_at >= time_threshold:
                    transfored_old_record = transform_record(old_tweet, update_user_metrics=False, enable_comprehend=False)
                else:
                    continue
        elif transfored_record['is_quote_status']:
            if 'quoted_status' in tweet:
                quote_count += 1
                old_tweet = tweet['quoted_status']
                old_created_at = int(datetime.strptime(old_tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
                if old_tweet['lang'] in twitter_langs and old_created_at >= time_threshold:
                    transfored_old_record = transform_record(old_tweet, update_user_metrics=False, enable_comprehend=False)
                else:
                    transfored_old_record = None
        else:
            transfored_old_record = None

        for rec in [transfored_record, transfored_old_record]:
            if rec:
                es_record = gen_es_record(rec)
                es_records.append({
                    'Data': json.dumps(es_record) + '\n',
                    'PartitionKey': rec['_id']
                })
    metrics.add_metric(name="ProcessingRecords", unit=MetricUnit.Count, value=tweet_count)
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
