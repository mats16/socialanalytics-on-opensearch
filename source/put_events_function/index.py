# -*- coding: utf-8 -*-
import base64
import json
import os
import boto3
import decimal
from datetime import datetime, timedelta, timezone
import logging

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch
patch(('boto3',))

event_bus_name = os.getenv('EventBusName')

client = boto3.client('events')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    entries = []
    for record in event['Records']:
        b64_data = record['kinesis']['data']
        record_string = base64.b64decode(b64_data).decode('utf-8').rstrip('\n')
        record_dict = json.loads(record_string)
        if 'comprehend' in record_dict and 'timestamp_ms' in record_dict:
            dtime = datetime.fromtimestamp(int(record_dict['timestamp_ms']) / 1000, timezone.utc)
            if dtime > (datetime.now(timezone.utc) - timedelta(minutes=10)):
                entries.append({
                    'EventBusName': event_bus_name,
                    'Time': dtime,
                    'Source': 'twitter.streaming',
                    #'Resources': [
                    #    'string',
                    #],
                    'DetailType': 'Analysed Tweet Data Ingestion',
                    'Detail': record_string,
                })
    if len(entries) > 0:
        res = client.put_events(Entries=entries)
        logger.info(res)
    return 'true'
