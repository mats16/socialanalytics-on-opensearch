# -*- coding: utf-8 -*-
import os
import boto3

cluster_name = os.getenv('CLUSTER_NAME')
service_name = os.getenv('SERVICE_NAME')

ecs = boto3.client('ecs')

def lambda_handler(event, context):
    print(event)
    res = ecs.update_service(
        cluster = cluster_name,
        service = service_name,
        forceNewDeployment=True
    )
    print(res)
