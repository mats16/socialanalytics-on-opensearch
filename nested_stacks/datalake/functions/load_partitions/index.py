# -*- coding: utf-8 -*-
import os
import boto3

database = os.getenv('DATABASE')
tables = os.getenv('TABLES').split(',')
workgroup = os.getenv('WORKGROUP')

athena = boto3.client('athena')

def lambda_handler(event, context):
    for table in tables:
        query = f'MSCK REPAIR TABLE {table};'
        # Execution
        res = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database,
                'Catalog': 'AwsDataCatalog'
            },
            WorkGroup=workgroup,
        )
    return
