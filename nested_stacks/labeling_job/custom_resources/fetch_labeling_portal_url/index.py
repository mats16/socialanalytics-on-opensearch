# -*- coding: utf-8 -*-
from __future__ import print_function
from crhelper import CfnResource
from aws_lambda_powertools import Logger
import json
import boto3

try:
    logger = Logger()
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    client = boto3.client('cognito-idp')
except Exception as e:
    helper.init_failure(e)
          
def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)
          
@helper.create
@helper.update
def fetch_labeling_portal_url(event, context):
    user_pool_id = event['ResourceProperties']['UserPoolId']
    client_id = event['ResourceProperties']['ClientId']
    res = client.describe_user_pool_client(
        UserPoolId=user_pool_id,
        ClientId=client_id
    )
    logout_url = res['UserPoolClient']['LogoutURLs'][0]
    labeling_portal_url = logout_url.rstrip('/logout')
    physical_resource_id = labeling_portal_url
    return physical_resource_id
                  
@helper.delete
def delete(event, context):
    return
