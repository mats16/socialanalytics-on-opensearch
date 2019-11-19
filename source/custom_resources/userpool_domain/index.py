from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')

try:
    client = boto3.client('cognito-idp')
    pass
except Exception as e:
    helper.init_failure(e)

@helper.create
def create(event, context):
    domain = event['ResourceProperties']['Domain']
    user_pool_id = event['ResourceProperties']['UserPoolId']
    response = client.create_user_pool_domain(
        Domain=domain,
        UserPoolId=user_pool_id,
    )
    logger.info(response)
    return response['CloudFrontDomain']

@helper.update
def update(event, context):
    domain = event['ResourceProperties']['Domain']
    user_pool_id = event['ResourceProperties']['UserPoolId']
    response = client.update_user_pool_domain(
        Domain=domain,
        UserPoolId=user_pool_id,
    )
    logger.info(response)
    return response['CloudFrontDomain']

@helper.delete
def delete(event, context):
    domain = event['ResourceProperties']['Domain']
    user_pool_id = event['ResourceProperties']['UserPoolId']
    response = client.delete_user_pool_domain(
        Domain=domain,
        UserPoolId=user_pool_id,
    )
    logger.info(response)

def handler(event, context):
    print(json.dumps(event))
    helper(event, context)
