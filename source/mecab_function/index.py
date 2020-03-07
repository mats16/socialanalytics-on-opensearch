# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import neologdn
import emoji
import re
from datetime import datetime
import logging
import zipfile
import MeCab

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with zipfile.ZipFile('/opt/neologd.zip') as neologd_zip:
    neologd_zip.extractall('/tmp')
    logger.info('unzip mecab dic to /tmp/neologd')

indexing_stream = os.getenv('INDEXING_STREAM')

def normalize(text):
    text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text_without_account)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_replaced_number.splitlines())
    return text_replaced_indention.lower()

def gen_index(prefix, dtime):
    return prefix + dtime.strftime('%Y-%m-%d')

def lambda_handler(event, context):
    tokenizer = MeCab.Tagger ('-d /tmp/neologd')
    tokenizer.parse('')
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

        if not is_retweeted and tweet['lang'] == 'ja':
            created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
            es_record = {
                'op_type': 'update',
                '_index': gen_index('tweets-', created_at),
                '_id': tweet['id_str'],
                'mecab': {},
            }
            normalized_text = normalize(tweet['text'])
            node = tokenizer.parseToNode(normalized_text)
            #mecab_wakati = []
            mecab_keywords = []
            while node:
                word = node.surface
                #mecab_wakati.append(word)
                w = node.feature.split(',')
                if w[0] == '名詞' and len(word) > 1:
                    logger.info(word + '\t' + str(w))
                    if w[1] in ['固有名詞', '一般']:
                        mecab_keywords.append(word)
                node = node.next
            if len(mecab_keywords):
                es_record['mecab']['keywords'] = list(set(mecab_keywords))
            #if len(mecab_wakati):
            #    es_record['mecab']['wakati'] = ' '.join(mecab_wakati)
            es_records.append({
                'Data': json.dumps(es_record) + '\n',
                'PartitionKey': es_record['_id']
            })

    if len(es_records) > 0:
        kinesis = boto3.client('kinesis')
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        logger.info(res)
    return 'true'