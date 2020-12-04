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
@helper.update
def create(event, context):
    stack_id = event['StackId']
    stack_name = event['StackId'].split('/')[1]
    logical_resource_id = event['LogicalResourceId']
    short_request_id = event['RequestId'].split('-')[-1]

    labeling_job_name = f'{logical_resource_id}-{short_request_id}'  # 自動生成
    label_attribute_name = event['ResourceProperties'].get('LabelAttributeName', logical_resource_id)
    human_task_title = f"{event['ResourceProperties']['HumanTaskTitle']} ({labeling_job_name})" if 'HumanTaskTitle' in event['ResourceProperties'] else labeling_job_name
    human_task_description = event['ResourceProperties'].get('HumanTaskDescription', human_task_title)
    input_topic_arn = event['ResourceProperties']['InputTopicArn']
    output_topic_arn = event['ResourceProperties']['OutputTopicArn']
    s3_output_path = event['ResourceProperties']['S3OutputPath']
    workteam_arn = event['ResourceProperties']['WorkteamArn']
    ui_template_s3_uri = event['ResourceProperties']['UiTemplateS3Uri']
    pre_human_task_lambda_arn = event['ResourceProperties']['PreHumanTaskLambdaArn']
    annotation_consolidation_lambda_arn = event['ResourceProperties']['AnnotationConsolidationLambdaArn']
    number_of_fuman_workers_per_data_object = int(event['ResourceProperties'].get('NumberOfHumanWorkersPerDataObject', 1))
    task_time_limit_in_seconds = int(event['ResourceProperties'].get('TaskTimeLimitInSeconds', 600))
    task_availability_lifetime_in_seconds = int(event['ResourceProperties'].get('TaskAvailabilityLifetimeInSeconds', 864000))  # default:10days
    max_concurrent_task_count = int(event['ResourceProperties'].get('MaxConcurrentTaskCount', 1000))

    res = sagemaker.create_labeling_job(
        LabelingJobName=labeling_job_name,
        LabelAttributeName=label_attribute_name,
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
        #LabelCategoryConfigS3Uri=label_category_config_s3_uri,
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
            'NumberOfHumanWorkersPerDataObject': number_of_fuman_workers_per_data_object,
            'TaskTimeLimitInSeconds': task_time_limit_in_seconds,
            'TaskAvailabilityLifetimeInSeconds': task_availability_lifetime_in_seconds,
            'MaxConcurrentTaskCount': max_concurrent_task_count,
            'AnnotationConsolidationConfig': {
                'AnnotationConsolidationLambdaArn': annotation_consolidation_lambda_arn
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
    helper.Data.update({ 'Arn': res['LabelingJobArn'] })
    physical_resource_id = labeling_job_name
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
