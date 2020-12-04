# -*- coding: utf-8 -*-
from __future__ import print_function
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Logger
from urllib.parse import urlparse
import boto3
import json
import statistics

tracer = Tracer()
logger = Logger()
s3 = boto3.client('s3')

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Post-annotation function
    https://docs.aws.amazon.com/ja_jp/sagemaker/latest/dg/sms-custom-templates-step2-demo2.html#sms-custom-templates-step2-demo2-post-lambda
    https://docs.aws.amazon.com/ja_jp/sagemaker/latest/dg/sms-custom-templates-step3.html#sms-custom-templates-step3-postlambda
    https://github.com/aws-samples/aws-sagemaker-ground-truth-recipe/blob/master/aws_sagemaker_ground_truth_sample_lambda/annotation_consolidation_lambda.py
    """
    label_attribute_name = event['labelAttributeName']

    parsed_url = urlparse(event['payload']['s3Uri'])
    textFile = s3.get_object(Bucket = parsed_url.netloc, Key = parsed_url.path[1:])
    filecont = textFile['Body'].read()
    annotations = json.loads(filecont)    

    consolidated_labels = []
    for dataset in annotations:
        dataset_object_id = dataset['datasetObjectId']

        labels = []
        for annotation in dataset['annotations']:
            content = json.loads(annotation['annotationData']['content'])
            label = content[label_attribute_name]['label']
            labels.append(label)
        mode_labels = statistics.multimode(labels)        
        mode_label = mode_labels[0] if len(mode_labels) == 1 else None  # Todo: check about None

        consolidated_label = {
            'datasetObjectId': dataset_object_id,
            'consolidatedAnnotation' : {
                'content': {
                    label_attribute_name: {
                        'label': mode_label,
                    }
                }
            }
        }
        consolidated_labels.append(consolidated_label)
    logger.info(consolidated_labels)
    return consolidated_labels
