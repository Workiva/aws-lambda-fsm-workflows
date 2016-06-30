#!/usr/bin/env python
#
# create_sqs_queue.py
#
# Script that creates an SQS Queue.

# system imports
import argparse
import logging
import sys

# library imports

# application imports
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string

import settings

# setup the command line args
parser = argparse.ArgumentParser(description='Creates AWS SQS queue.')
parser.add_argument('--sqs_queue_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--log_level', default='INFO')
parser.add_argument('--boto_log_level', default='INFO')
args = parser.parse_args()

logging.basicConfig(
    format='[%(levelname)s] %(asctime)-15s %(message)s',
    level=int(args.log_level) if args.log_level.isdigit() else args.log_level,
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.getLogger('boto3').setLevel(args.boto_log_level)
logging.getLogger('botocore').setLevel(args.boto_log_level)

# setup connections to AWS
sqs_queue_arn = getattr(settings, args.sqs_queue_arn)
logging.info('SQS queue ARN: %s', sqs_queue_arn)
logging.info('SQS endpoint: %s', settings.ENDPOINTS.get(AWS.SQS))
if get_arn_from_arn_string(sqs_queue_arn).service != AWS.SQS:
    logging.fatal("%s is not an SQS ARN", sqs_queue_arn)
    sys.exit(1)
sqs_conn = get_connection(sqs_queue_arn)
sqs_queue = get_arn_from_arn_string(sqs_queue_arn).resource
logging.info('SQS queue: %s', sqs_queue)

# configure the queue
response = sqs_conn.create_queue(
    QueueName=sqs_queue
)
logging.info(response)
