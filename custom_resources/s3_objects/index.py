# -*- coding: utf-8 -*-
from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import os
import boto3
import urllib3
import zipfile

try:
    logger = logging.getLogger(__name__)
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    request_methods = urllib3.PoolManager()
    _lambda = boto3.client('lambda')
    _s3 = boto3.client('s3')
except Exception as e:
    helper.init_failure(e)

def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
@helper.update
def create(event, context):
    bucket = event['ResourceProperties']['Bucket']
    path = event['ResourceProperties'].get('Path', '')
    key = event['ResourceProperties'].get('Key', None)
    acl = event['ResourceProperties'].get('ACL', 'private')
    source_layer_arn = event['ResourceProperties']['SourceLayerArn']

    layer = _lambda.get_layer_version_by_arn(Arn=source_layer_arn)
    layer_content_location = layer['Content']['Location']
    layer_version = layer['Version']
    path_with_version = f'{path}v{layer_version}/'
    tmp_f = '/tmp/layer.zip'
    with open(tmp_f, 'wb') as f:
        res = request_methods.request('GET', layer_content_location)
        f.write(res.data)
    if key:
        with open(tmp_f, 'rb') as f:
            res = _s3.put_object(
                Bucket=bucket,
                Key=f'{path_with_version}{key}',
                Body=f,
                ACL=acl,
            )
    else:
        fmp_dir = '/tmp/layer/'
        with zipfile.ZipFile('/tmp/layer.zip') as zf:
            list_f = zf.namelist()
            zf.extractall(fmp_dir)
        for fn in list_f:
            with open(f'{fmp_dir}{fn}', 'rb') as f:
                res = _s3.put_object(
                    Bucket=bucket,
                    Key=f'{path_with_version}{fn}',
                    Body=f,
                    ACL=acl,
                )
    if key:
        physical_resource_id = f's3://{bucket}/{path_with_version}{key}'
        helper.Data.update({ 'Bucket': bucket, 'Path': f'{path_with_version}{key}' })
    else:
        physical_resource_id = f's3://{bucket}/{path_with_version}'
        helper.Data.update({ 'Bucket': bucket, 'Path': path_with_version })
    return physical_resource_id

@helper.delete
def delete(event, context):
    pass