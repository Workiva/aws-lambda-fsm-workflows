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
# create_resources.py
#
# Script that creates all the requires reources specified in settings.py

# system imports
import subprocess
import logging

# library imports

# application imports
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.aws import validate_config
import settings

logging.basicConfig(level=logging.INFO)
validate_config()

for attr in dir(settings):
    if attr.startswith('PRIMARY_') or attr.startswith('SECONDARY_') or attr.startswith('RESULTS_'):
        arn_string = getattr(settings, attr)
        arn = get_arn_from_arn_string(arn_string)
        if arn.service:
            logging.info('*' * 80)
            logging.info('CREATING %s', arn_string)
            logging.info('*' * 80)
            if arn.service == AWS.KINESIS:
                subprocess.call(['create_kinesis_stream.py', '--kinesis_stream_arn=' + attr])
            elif arn.service == AWS.DYNAMODB:
                subprocess.call(['create_dynamodb_table.py', '--dynamodb_table_arn=' + attr])
            elif arn.service == AWS.SNS:
                subprocess.call(['create_sns_topic.py', '--sns_topic_arn=' + attr])
            elif arn.service == AWS.SQS:
                subprocess.call(['create_sqs_queue.py', '--sqs_queue_arn=' + attr])
