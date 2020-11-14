# -*- coding: utf-8 -*-
from __future__ import print_function
from crhelper import CfnResource
import os
import logging
import json
import boto3

try:
    logger = logging.getLogger(__name__)
    helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL')
    ground_truth_role_arn = os.environ['GROUND_TRUTH_ROLE_ARN']
    region = os.environ['AWS_REGION']
    sagemaker = boto3.client('sagemaker')
    sqs = boto3.client('sqs')
except Exception as e:
    helper.init_failure(e)
          
def lambda_handler(event, context):
    print(json.dumps(event))
    helper(event, context)

@helper.create
def create(event, context):
    stack_id = event['StackId']
    stack_name = event['StackId'].split('/')[1]
    logical_resource_id = event['LogicalResourceId']
    request_id = event['RequestId']

    labeling_job_name = logical_resource_id + '-' + request_id.split('-')[-1]
    human_task_title = event['ResourceProperties']['HumanTaskTitle']
    human_task_description = event['ResourceProperties']['HumanTaskDescription']
    pre_human_task_lambda_arn = event['ResourceProperties']['PreHumanTaskLambdaArn']
    input_topic_arn = event['ResourceProperties']['InputTopicArn']
    output_topic_arn = event['ResourceProperties']['OutputTopicArn']
    s3_output_path = event['ResourceProperties']['S3OutputPath']
    label_category_config_s3_uri = event['ResourceProperties']['LabelCategoryConfigS3Uri']
    workteam_arn = event['ResourceProperties']['WorkteamArn']
    ui_template_s3_uri = event['ResourceProperties']['UiTemplateS3Uri']

    res = sagemaker.create_labeling_job(
        LabelingJobName=labeling_job_name,
        LabelAttributeName=labeling_job_name,
        InputConfig={
            'DataSource': {
                'SnsDataSource': {
                    'SnsTopicArn': input_topic_arn
                }
            },
        },
        OutputConfig={
            'SnsTopicArn': output_topic_arn,
            'S3OutputPath': s3_output_path
        },
        RoleArn=ground_truth_role_arn,
        LabelCategoryConfigS3Uri=label_category_config_s3_uri,
        #StoppingConditions={
        #    'MaxHumanLabeledObjectCount': 123,
        #    'MaxPercentageOfInputDatasetLabeled': 123
        #},
        HumanTaskConfig={
            'TaskTitle': human_task_title,
            'TaskDescription': human_task_description,
            'WorkteamArn': workteam_arn,
            'UiConfig': {
                'UiTemplateS3Uri': ui_template_s3_uri,
            },
            'PreHumanTaskLambdaArn': pre_human_task_lambda_arn,
            'NumberOfHumanWorkersPerDataObject': 1,
            'TaskTimeLimitInSeconds': 600,
            'TaskAvailabilityLifetimeInSeconds': 864000, # default:10days
            'MaxConcurrentTaskCount': 1000,  # default
            'AnnotationConsolidationConfig': {
                'AnnotationConsolidationLambdaArn': 'arn:aws:lambda:us-west-2:081040173940:function:ACS-TextMultiClass'
            },
        },
        Tags=[
            {
                'Key': 'cloudformation:stack-name',
                'Value': stack_id
            },
            {
                'Key': 'cloudformation:stack-id',
                'Value': stack_name
            },
            {
                'Key': 'cloudformation:logical-id',
                'Value': logical_resource_id
            },
        ]
    )

    physical_resource_id = labeling_job_name
    return physical_resource_id

@helper.update
def update(event, context):
    try:
        old_labeling_job_name = event['PhysicalResourceId']
        sagemaker.stop_labeling_job(
            LabelingJobName=old_labeling_job_name
        )
        account_id = event['ServiceToken'].split(':')[4]
        old_queue_name = 'GroundTruth-' + old_labeling_job_name.lower()
        queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{old_queue_name}'
        sqs.delete_queue(
            QueueUrl=queue_url
        )
    except Exception as e:
        logger.error(e)
    physical_resource_id = create(event, context)
    return physical_resource_id
                  
@helper.delete
def delete(event, context):
    try:
        labeling_job_name = event['PhysicalResourceId']
        sagemaker.stop_labeling_job(
            LabelingJobName=labeling_job_name
        )
        account_id = event['ServiceToken'].split(':')[4]
        queue_name = 'GroundTruth-' + labeling_job_name.lower()
        queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'
        sqs.delete_queue(
            QueueUrl=queue_url
        )
    except Exception as e:
        logger.error(e)
