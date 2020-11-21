# -*- coding: utf-8 -*-
from aws_lambda_powertools import Logger
import json
import os

logger = Logger()

label_attribute_name = os.getenv('LabelAttributeName')
allowed_label_value = os.getenv('AllowedLabelValue')

def lambda_handler(event, context):
    """Pre-annotation function
    https://docs.aws.amazon.com/sagemaker/latest/dg/sms-custom-templates-step3.html#sms-custom-templates-step3-prelambda
    """
    if label_attribute_name and allowed_label_value:
        label = event['dataObject'].get(label_attribute_name, {}).get('label')
        is_human_annotation_required = True if label == allowed_label_value else False
    else:
        is_human_annotation_required = True

    try:
        task_input = json.loads(event['dataObject']['source'])
    except Exception as e:
        logger.error(e)
        is_human_annotation_required = False

    output = {
        "taskInput": task_input,
        "isHumanAnnotationRequired": is_human_annotation_required,
    }
    return output
