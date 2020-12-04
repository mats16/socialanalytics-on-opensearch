# -*- coding: utf-8 -*-
from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3

try:
    logger = logging.getLogger(__name__)
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    client = boto3.client('logs')
except Exception as e:
    helper.init_failure(e)
          
def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
def create(event, context):
    policy_name = event['ResourceProperties']['PolicyName']
    policy_document = event['ResourceProperties']['PolicyDocument']
    res = client.put_resource_policy(
        policyName=policy_name,
        policyDocument=policy_document
    )
    physical_resource_id = res['resourcePolicy']['policyName']
    return physical_resource_id

@helper.update
def update(event, context):
    old_resource_properties = event['OldResourceProperties']
    old_policy_name = old_resource_properties['PolicyName']
    policy_name = event['ResourceProperties']['PolicyName']
    policy_document = event['ResourceProperties']['PolicyDocument']
    if old_policy_name != policy_name:
        res = client.delete_resource_policy(
            policyName=old_policy_name
        )
    res = client.put_resource_policy(
        policyName=policy_name,
        policyDocument=policy_document
    )
    physical_resource_id = res['resourcePolicy']['policyName']
    return physical_resource_id
                  
@helper.delete
def delete(event, context):
    policy_name = event['PhysicalResourceId']
    try:
        res = client.delete_resource_policy(
            policyName=policy_name
        )
    except Exception as e:
        logger.error(e)
