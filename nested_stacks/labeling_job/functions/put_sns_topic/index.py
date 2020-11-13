# -*- coding: utf-8 -*-
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit
import base64
import json
import os
import boto3
from datetime import datetime, timedelta, timezone

service_name = os.getenv('SERVICE_NAME')
input_topic_arn = os.getenv('INPUT_TOPIC_ARN')
tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))

logger = Logger(service=service_name)
tracer = Tracer(service=service_name)
#metrics = Metrics(namespace="SocialMediaDashboard")

sns = boto3.client('sns')

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info({
        'state': 'get_records',
        'record_count': len(event['Records']),
        'text': 'Get records from kinesis'
    })
    records = set(map(lambda x: x['kinesis']['data'], event['Records']))  # 重複排除
    logger.info({
        'state': 'deduplicate_records',
        'record_count': len(records),
        'text': 'Deduplicate records'
    })
    es_records = []
    for record in records:
        tweet_string = base64.b64decode(record).decode('utf-8').rstrip('\n')
        tweet = json.loads(tweet_string)

        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        time_threshold = int((datetime.now(timezone.utc) - timedelta(days=tweet_day_threshold)).timestamp())
        if created_at < time_threshold:
            continue  # 365日以上前の場合はスキップ
        elif 'retweeted_status' in tweet:
            continue  # retweetの場合はスキップ

        # https://docs.aws.amazon.com/sagemaker/latest/dg/sms-streaming-labeling-job.html#sms-streaming-impotency
        tweet['dataset-objectid-attribute-name'] = 'id_str'

        res = sns.publish(
            TopicArn=input_topic_arn,
            Message=json.dumps(tweet),
            #Subject='string',
            #MessageStructure='json',
            #MessageAttributes={
            #    'string': {
            #        'DataType': 'string',
            #        'StringValue': 'string',
            #        'BinaryValue': b'bytes'
            #    }
            #},
            #MessageDeduplicationId='string',
            #MessageGroupId='string'
        )

    return 'true'