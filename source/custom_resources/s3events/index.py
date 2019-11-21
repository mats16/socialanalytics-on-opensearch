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

def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
def create(event, context):
    resource_id = event['LogicalResourceId']
    bucket_name = event['ResourceProperties']['BucketName']
    events = event['ResourceProperties']['Events']
    prefix = event['ResourceProperties']['Prefix']
    function_arn = event['ResourceProperties']['FunctionArn']
    account_id = context.invoked_function_arn.split(':')[4]
    boto3.client('lambda').add_permission(
        FunctionName=function_arn,
        StatementId=resource_id,
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn='arn:aws:s3:::{0}'.format(bucket_name),
        SourceAccount=account_id,
    )
    boto3.client('s3').put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'Id': resource_id,
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

@helper.update
def update(event, context):
    resource_id = event['LogicalResourceId']
    bucket_name = event['ResourceProperties']['BucketName']
    events = event['ResourceProperties']['Events']
    prefix = event['ResourceProperties']['Prefix']
    function_arn = event['ResourceProperties']['FunctionArn']
    account_id = context.invoked_function_arn.split(':')[4]
    boto3.client('lambda').remove_permission(
        FunctionName=function_arn,
        StatementId=resource_id,
    )
    boto3.client('lambda').add_permission(
        FunctionName=function_arn,
        StatementId=resource_id,
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn='arn:aws:s3:::{0}'.format(bucket_name),
        SourceAccount=account_id,
    )
    boto3.client('s3').put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'Id': resource_id,
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

@helper.delete
def delete(event, context):
    resource_id = event['LogicalResourceId']
    function_arn = event['ResourceProperties']['FunctionArn']
    boto3.client('lambda').remove_permission(
        FunctionName=function_arn,
        StatementId=resource_id,
    )
    logger.info("There is nothing to do, because the API to delete is not supported yet. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html")
