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

input_topic_arn = os.getenv('INPUT_TOPIC_ARN')
tweet_day_threshold = int(os.getenv('TWEET_DAY_THRESHOLD'))

logger = Logger()
tracer = Tracer()
metrics = Metrics()

sns = boto3.client('sns')

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
    metrics.add_metric(name="DistinctIncomingRecords", unit=MetricUnit.Count, value=len(distinct_json_records))

    time_threshold = int((datetime.now(timezone.utc) - timedelta(days=tweet_day_threshold)).timestamp())
    record_count = 0
    for tweet in distinct_json_records:
        created_at = int(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp())
        if created_at < time_threshold:
            continue  # 365日以上前の場合はスキップ
        elif 'retweeted_status' in tweet:
            continue  # retweetの場合はスキップ

        # https://docs.aws.amazon.com/sagemaker/latest/dg/sms-streaming-labeling-job.html#sms-streaming-impotency
        data = {
            'source': json.dumps(tweet),
            'dataset-objectid-attribute-name': 'tweet_id',
            'tweet_id': tweet['id_str'],
        }
        res = sns.publish(
            TopicArn=input_topic_arn,
            Message=json.dumps(data),
        )
        record_count += 1
    metrics.add_metric(name="OutgoingRecords", unit=MetricUnit.Count, value=record_count)
    return 'true'