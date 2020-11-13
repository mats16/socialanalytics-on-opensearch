# -*- coding: utf-8 -*-

def lambda_handler(event, context):
    tweet = event['dataObject']
    output = {
        "taskInput": tweet,
        "isHumanAnnotationRequired": "true"
    }
    return output