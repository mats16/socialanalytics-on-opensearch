# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import neologdn
import emoji
import re
from datetime import datetime, timedelta, timezone
import logging
import zipfile
import MeCab
import termextract.mecab
import termextract.core
from collections import Counter

indexing_stream = os.getenv('INDEXING_STREAM')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with zipfile.ZipFile('/opt/neologd.zip') as neologd_zip:
    neologd_zip.extractall('/tmp')
    logger.info('unzip mecab dic to /tmp/neologd')
mecab = MeCab.Tagger ('-Ochasen -d /tmp/neologd')
mecab.parse('')

def get_mecab_keywords(text):
    keywords = []
    node = mecab.parseToNode(text)
    while node:
        if node.surface:
            word = node.surface
            tags = node.feature.split(',')
            if tags[0] == '名詞' and len(word) > 1:
                if tags[1] == '固有名詞':
                    keywords.append(word)
        node = node.next
    return list(set(keywords))

def get_mecab_tagged(text):
    node = mecab.parseToNode(text)
    buf = ''
    while node:
        if node.surface:
            buf += node.surface + '\t' + node.feature + '\n'
        node = node.next
    return buf

def term_ext(tagged_text):
    frequency = termextract.mecab.cmp_noun_dict(tagged_text)
    lr = termextract.core.score_lr(
        frequency,
        ignore_words=termextract.mecab.IGNORE_WORDS,
        lr_mode=1, average_rate=1)
    term_imp = termextract.core.term_importance(frequency, lr)
    return Counter(term_imp)

def remove_single_words(terms):
    c = Counter()
    for cmp, value in terms.items():
        if len(cmp.split(' ')) != 1:
            c[termextract.core.modify_agglutinative_lang(cmp)] = value
    return c

def normalize(text):
    text_without_account = re.sub(r'@[a-zA-Z0-9_]+', '', text)  # remove twitter_account
    text_without_url = re.sub(r'https?://[\w/;:%#\$&\?\(\)~\.=\+\-]+', '', text_without_account)  # remove URL
    text_normalized = neologdn.normalize(text_without_url).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    text_without_emoji = ''.join(['' if c in emoji.UNICODE_EMOJI else c for c in text_normalized])
    tmp = re.sub(r'(\d)([,.])(\d+)', r'\1\3', text_without_emoji)
    text_replaced_number = re.sub(r'\d+', '0', tmp)
    text_replaced_indention = ' '.join(text_replaced_number.splitlines())
    return text_replaced_indention.lower()

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

        created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
        if created_at < (datetime.now(timezone.utc) - timedelta(days=365)):
            continue  # 365日以上前の場合はスキップ

        if not is_retweeted and tweet['lang'] == 'ja':
            es_record = {
                'id_str': tweet['id_str'],
                'timestamp_ms': tweet.get('timestamp_ms', str(int(created_at.timestamp()) * 1000)),  #　retweet の場合、timestamp_ms が存在しない。元のフォーマットに合わせて string にする。
                'mecab': {},
            }
            keywords = []
            normalized_text = normalize(tweet['text'])
            mecab_keywords = get_mecab_keywords(normalized_text)
            if len(mecab_keywords):
                logger.info(mecab_keywords)
                keywords = keywords + mecab_keywords

            tagged_text = get_mecab_tagged(normalized_text)
            terms = term_ext(tagged_text)
            compound = remove_single_words(terms)
            if len(compound):
                logger.info(compound)
                keywords = keywords + list(compound.keys())

            if len(keywords):
                es_record['mecab']['keywords'] = list(set(keywords))
            es_records.append({
                'Data': json.dumps(es_record) + '\n',
                'PartitionKey': es_record['id_str']
            })

    if len(es_records) > 0:
        kinesis = boto3.client('kinesis')
        res = kinesis.put_records(
            Records=es_records,
            StreamName=indexing_stream
        )
        logger.info(res)
    return 'true'