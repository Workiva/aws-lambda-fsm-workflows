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
# start_state_machine.py
#
# Script that injects an initial event into Kinesis.

# system imports
import argparse
import json
import logging
import sys

# library imports

# application imports
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.aws import validate_config
from aws_lambda_fsm.client import start_state_machine
from aws_lambda_fsm.client import start_state_machines
from aws_lambda_fsm.constants import STATE
from aws_lambda_fsm.constants import SYSTEM_CONTEXT
from aws_lambda_fsm.constants import AWS_KINESIS
from aws_lambda_fsm.constants import AWS

import settings

# setup the command line args
parser = argparse.ArgumentParser(description='Starts a state machine.')
parser.add_argument('--machine_name')
parser.add_argument('--checkpoint_shard_id')
parser.add_argument('--checkpoint_sequence_number')
parser.add_argument('--initial_context')
parser.add_argument('--num_machines', type=int, default=1)
parser.add_argument('--log_level', default='INFO')
parser.add_argument('--boto_log_level', default='INFO')
parser.add_argument('--correlation_id')
args = parser.parse_args()

logging.basicConfig(
    format='[%(levelname)s] %(asctime)-15s %(message)s',
    level=int(args.log_level) if args.log_level.isdigit() else args.log_level,
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.getLogger('boto3').setLevel(args.boto_log_level)
logging.getLogger('botocore').setLevel(args.boto_log_level)

validate_config()

if args.num_machines > 1:
    # start things off
    context = json.loads(args.initial_context or "{}")
    current_state = current_event = STATE.PSEUDO_INIT
    start_state_machines(args.machine_name,
                         [context] * args.num_machines,
                         current_state=current_state,
                         current_event=current_event)
    exit(0)

# checkpoint specified, so start with a context saved to the kinesis stream
if args.checkpoint_shard_id and args.checkpoint_sequence_number:

    # setup connections to AWS
    kinesis_stream_arn = getattr(settings, args.kinesis_stream_arn)
    logging.info('Kinesis stream ARN: %s', kinesis_stream_arn)
    logging.info('Kinesis endpoint: %s', settings.ENDPOINTS.get(AWS.KINESIS))
    if get_arn_from_arn_string(kinesis_stream_arn).service != AWS.KINESIS:
        logging.fatal("%s is not a Kinesis ARN", kinesis_stream_arn)
        sys.exit(1)
    kinesis_conn = get_connection(kinesis_stream_arn)
    kinesis_stream = get_arn_from_arn_string(kinesis_stream_arn).slash_resource()
    logging.info('Kinesis stream: %s', kinesis_stream)

    # create a shard iterator for the specified shard and sequence number
    shard_iterator = kinesis_conn.get_shard_iterator(
        StreamName=kinesis_stream,
        ShardId=args.checkpoint_shard_id,
        ShardIteratorType=AWS_KINESIS.AT_SEQUENCE_NUMBER,
        StartingSequenceNumber=args.checkpoint_sequence_number
    )[AWS_KINESIS.ShardIterator]

    # get the record that has the last successful state
    records = kinesis_conn.get_records(
        ShardIterator=shard_iterator,
        Limit=1
    )
    if records:
        context = json.loads(records[AWS_KINESIS.Records][0][AWS_KINESIS.DATA])
        current_state = context.get(SYSTEM_CONTEXT.CURRENT_STATE)
        current_event = context.get(SYSTEM_CONTEXT.CURRENT_EVENT)
    else:
        context = {}

# no checkpoint specified, so start with an empty context
else:
    context = json.loads(args.initial_context or "{}")
    current_state = current_event = STATE.PSEUDO_INIT

# start things off
start_state_machine(args.machine_name,
                    context,
                    correlation_id=args.correlation_id,
                    current_state=current_state,
                    current_event=current_event)
