# -*- coding: utf-8 -*-
from __future__ import print_function
from crhelper import CfnResource
import logging
import json
import boto3

try:
    logger = logging.getLogger(__name__)
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    region = os.environ['AWS_REGION']
    client = boto3.client('sagemaker')
except Exception as e:
    helper.init_failure(e)
          
def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
def create(event, context):
    sns_topic_arn = event['ResourceProperties']['SnsTopicArn']
    s3_output_path = event['ResourceProperties']['S3OutputPath']
    workteam_arn = event['ResourceProperties']['WorkteamArn']
    ui_template_s3_uri = event['ResourceProperties']['UiTemplateS3Uri']

    res = client.create_labeling_job(
        LabelingJobName='string',
        LabelAttributeName='string',
        InputConfig={
            'DataSource': {
                'SnsDataSource': {
                    'SnsTopicArn': sns_topic_arn
                }
            },
            #'DataAttributes': {
            #    'ContentClassifiers': [
            #        'FreeOfPersonallyIdentifiableInformation'|'FreeOfAdultContent',
            #    ]
            #}
        },
        OutputConfig={
            'S3OutputPath': s3_output_path,
        },
        RoleArn='string',
        #LabelCategoryConfigS3Uri='string',
        #StoppingConditions={
        #    'MaxHumanLabeledObjectCount': 123,
        #    'MaxPercentageOfInputDatasetLabeled': 123
        #},
        LabelingJobAlgorithmsConfig={
            'LabelingJobAlgorithmSpecificationArn': f'arn:aws:sagemaker:{region}:027400017018:labeling-job-algorithm-specification/text-classification',
            #'InitialActiveLearningModelArn': 'string',
            #'LabelingJobResourceConfig': {
            #    'VolumeKmsKeyId': 'string'
            #}
        },
        HumanTaskConfig={
            'WorkteamArn': workteam_arn,
            'UiConfig': {
                'UiTemplateS3Uri': ui_template_s3_uri,
                #'HumanTaskUiArn': ''
            },
            'PreHumanTaskLambdaArn': 'arn:aws:lambda:us-west-2:081040173940:function:PRE-TextMultiClass',
            #'TaskKeywords': [
            #    'string',
            #],
            'TaskTitle': 'string',
            'TaskDescription': 'string',
            'NumberOfHumanWorkersPerDataObject': 1,
            'TaskTimeLimitInSeconds': 300,
            #'TaskAvailabilityLifetimeInSeconds': 123,
            #'MaxConcurrentTaskCount': 123,
            'AnnotationConsolidationConfig': {
                'AnnotationConsolidationLambdaArn': 'arn:aws:lambda:us-west-2:081040173940:function:ACS-TextMultiClass'
            },
            #'PublicWorkforceTaskPrice': {
            #    'AmountInUsd': {
            #        'Dollars': 123,
            #        'Cents': 123,
            #        'TenthFractionsOfACent': 123
            #    }
            #}
        },
        #Tags=[
        #    {
        #        'Key': 'string',
        #        'Value': 'string'
        #    },
        #]
    )

    physical_resource_id = res['LabelingJobArn']
    return physical_resource_id

@helper.update
def update(event, context):
    physical_resource_id = event['PhysicalResourceId']
    return physical_resource_id
                  
@helper.delete
def delete(event, context):
    policy_name = event['PhysicalResourceId']
    try:
        pass
    except Exception as e:
        logger.error(e)
