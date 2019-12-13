# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import neologdn
import re
import emoji
from datetime import datetime
import zipfile
import logging
from sudachipy.tokenizer import Tokenizer
from sudachipy.dictionary import Dictionary

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with zipfile.ZipFile('./sudachi-dictionary-20191030-full.zip') as sudachi_dic_zip:
    sudachi_dic_zip.extractall('/tmp')
    logger.info('unzip sudachi dic to /tmp/sudachi-dictionary-20191030-full')

tokenizer_obj = Dictionary().create()
mode = Tokenizer.SplitMode.C

indexing_stream = os.environ['INDEXING_STREAM']

def normalize(text):
    text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/:%#\$&\?\(\)~\.=\+\-]+', '', text_without_account)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_replaced_number.splitlines())
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

        if not is_retweeted and tweet['lang'] == 'ja':
            created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
            es_record = {
                'op_type': 'update',
                '_index': gen_index('tweets-', created_at),
                '_id': tweet['id_str'],
                'sudachi': {},
            }
            normalized_text = normalize(tweet['text'])
            #sudachi_wakati = []
            sudachi_keywords = []
            for m in tokenizer_obj.tokenize(normalized_text, mode):
                if m.surface() == ' ':
                    continue
                #sudachi_wakati.append(m.surface())
                if m.part_of_speech()[0] == '名詞' and len(m.surface()) > 1:
                    logger.info(m.surface() + '\t' + str(m.part_of_speech()))
                    if m.part_of_speech()[1] == '固有名詞' or (m.part_of_speech()[1] == '普通名詞' and m.part_of_speech()[2] == '一般'):
                        sudachi_keywords.append(m.surface())
            if len(sudachi_keywords):
                es_record['sudachi']['keywords'] = list(set(sudachi_keywords))
            #if len(sudachi_wakati):
            #    es_record['sudachi']['wakati'] = ' '.join(sudachi_wakati)
            logger.info(es_record)
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