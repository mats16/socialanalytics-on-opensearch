# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import boto3
from aws_lambda_powertools import Logger

group_name = os.environ.get('GROUP_NAME')
logger = Logger()
client = boto3.client('cognito-idp')

def lambda_handler(event, context):
    logger.info(event)
    user_pool_id = event['userPoolId']
    user_name = event['userName']
    try:
        res = client.admin_add_user_to_group(
            UserPoolId=user_pool_id,
            Username=user_name,
            GroupName=group_name
        )
        logger.info(res)
    except Exception as e:
        logger.error(res)
    # Return to Amazon Cognito
    return event