# -*- coding: utf-8 -*-
from __future__ import print_function
import os
from aws_lambda_powertools import Logger

allowed_domains = os.environ.get('ALLOWED_DOMAINS')
logger = Logger()

def lambda_handler(event, context):
    logger.info(event)
    email = event['request']['userAttributes']['email']
    domain = email.split('@')[1]
    if allowed_domains == '*':
        pass
    elif domain in allowed_domains.split(','):
        pass
    else:
        raise Exception('@{0} is not allowed to regiter.'.format(domain))
    return event
