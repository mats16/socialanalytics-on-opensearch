import json
import boto3
import os

from aws_xray_sdk.core import patch
patch(['boto3'])

src_prefix = 'queued/'
dest_prefix = 'processed/'
dest_stream = os.environ['ANALYZE_STREAM']

s3 = boto3.resource('s3')
kinesis = boto3.client('kinesis')

def lambda_handler(event, context):
    print(event)
    
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        
        obj = s3.Object(s3_bucket, s3_key)
        tweets = obj.get()['Body'].read().decode('utf-8').split('\n')

        records = []
        for tweet_string in tweets:
            
            if len(tweet_string) < 1:
                continue
            else:
                tweet = json.loads(tweet_string)
                records.append({
                    'Data': json.dumps(tweet) + '\n',
                    'PartitionKey': tweet['id_str']
                })

        if len(records) > 0:
            response = kinesis.put_records(
                Records=records,
                StreamName=dest_stream
            )
            print(response)

        new_s3_key = s3_key.replace(src_prefix, dest_prefix)
        s3.Bucket(s3_bucket).Object(new_s3_key).copy({'Bucket': s3_bucket, 'Key': s3_key})
        obj.delete()

    return 'true'