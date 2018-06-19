#!/usr/bin/env python

# Copyright 2016-2018 Workiva Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# create_sns_topic.py
#
# Script that create an SNS Topic.

# system imports
import argparse
import logging
import sys

# library imports

# application imports
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.aws import validate_config

import settings

# setup the command line args
parser = argparse.ArgumentParser(description='Creates AWS SNS topics.')
parser.add_argument('--sns_topic_arn', default='PRIMARY_STREAM_SOURCE')
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

validate_config()

# setup connections to AWS
sns_topic_arn = getattr(settings, args.sns_topic_arn)
logging.info('SNS topic ARN: %s', sns_topic_arn)
logging.info('SNS endpoint: %s', settings.ENDPOINTS.get(AWS.SNS))
if get_arn_from_arn_string(sns_topic_arn).service != AWS.SNS:
    logging.fatal("%s is not an SNS ARN", sns_topic_arn)
    sys.exit(1)
sns_conn = get_connection(sns_topic_arn, disable_chaos=True)
sns_topic = get_arn_from_arn_string(sns_topic_arn).resource
logging.info('SNS topic: %s', sns_topic)

# configure the topic
response = sns_conn.create_topic(
    Name=sns_topic
)
logging.info(response)
