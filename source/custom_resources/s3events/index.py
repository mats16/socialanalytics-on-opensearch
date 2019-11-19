from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')

try:
    ## Init code goes here
    pass
except Exception as e:
    helper.init_failure(e)

@helper.create
@helper.update
def create(event, context):
    logger.info('RequestType is {0}.'.format(event['RequestType']))
    bucket_name = event['ResourceProperties']['BucketName']
    events = event['ResourceProperties']['Events']
    prefix = event['ResourceProperties']['Prefix']
    function_arn = event['ResourceProperties']['FunctionArn']
    response1 = boto3.client('lambda').add_permission(
        FunctionName=function_arn,
        StatementId='S3callingLambdaForSocialMedia',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn='arn:aws:s3:::{0}'.format(bucket_name),
    )
    logger.info(response1)
    response2 = boto3.client('s3').put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': function_arn,
                    'Events': events,
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': prefix
                                },
                            ]
                        }
                    }
                },
            ]
        }
    )
    logger.info(response2)

@helper.delete
def delete(event, context):
    logger.info("There is nothing to do, because the API to delete is not supported yet. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html")

def handler(event, context):
    print(json.dumps(event))
    helper(event, context)
