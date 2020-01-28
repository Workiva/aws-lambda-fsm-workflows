#!/usr/bin/env python

# Copyright 2016-2020 Workiva Inc.
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
# dev_lambda.py
#
# Script that pretends to be AWS Lambda attached to a AWS Kinesis Stream.

# system imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
import time
import base64
import logging
import argparse
import random
import http.server
import json
import sys
import subprocess

# setup the command line args very early in the process because
# they are need to setup the log level in basicConfig, and that
# must happen before aws_lambda_fsm code is imported (see below)
parser = argparse.ArgumentParser(description='Mock AWS Lambda service.')
parser.add_argument('--kinesis_stream_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--dynamodb_table_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--sns_topic_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--sqs_queue_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--log_level', default='INFO')
parser.add_argument('--boto_log_level', default='INFO')
parser.add_argument('--lambda_batch_size', type=int, default=100)
parser.add_argument('--sleep_time', type=float, default=0.2)
parser.add_argument('--run_kinesis_lambda', type=int, default=0)
parser.add_argument('--run_sqs_lambda', type=int, default=0)
parser.add_argument('--run_dynamodb_lambda', type=int, default=0)
parser.add_argument('--run_timer_lambda', type=int, default=0)
parser.add_argument('--run_sns_lambda', type=int, default=0)
parser.add_argument('--random_seed', type=int, default=0)
parser.add_argument('--lambda_command', help='command to run lambda code (eg. docker run -v ' +
                                             '"$PWD":/var/task lambci/lambda:python2.7 main.lambda_handler)')
args = parser.parse_args()

if (not args.run_kinesis_lambda) and \
   (not args.run_sqs_lambda) and \
   (not args.run_dynamodb_lambda) and \
   (not args.run_timer_lambda) and \
   (not args.run_sns_lambda):
    parser.error("One of --run_(kinesis|sqs|dynamodb|sns|timer)_lambda is required.")

# setup the logger BEFORE any aws_lambda_fsm code is imported otherwise that code emits
# 'No handlers could be found for logger "aws_lambda_fsm.aws"'
logging.basicConfig(
    format='[%(levelname)s] %(asctime)-15s %(message)s',
    level=int(args.log_level) if args.log_level.isdigit() else args.log_level,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger('boto3').setLevel(args.boto_log_level)
logging.getLogger('botocore').setLevel(args.boto_log_level)

# library imports

# application imports
from aws_lambda_fsm.handler import lambda_handler        # noqa: E402
from aws_lambda_fsm.handler import lambda_timer_handler  # noqa: E402
from aws_lambda_fsm.aws import get_connection            # noqa: E402
from aws_lambda_fsm.aws import get_arn_from_arn_string   # noqa: E402
from aws_lambda_fsm.aws import validate_config           # noqa: E402
from aws_lambda_fsm.aws import _get_sqs_queue_url        # noqa: E402
from aws_lambda_fsm.constants import AWS_KINESIS         # noqa: E402
from aws_lambda_fsm.constants import AWS_DYNAMODB        # noqa: E402
from aws_lambda_fsm.constants import AWS_LAMBDA          # noqa: E402
from aws_lambda_fsm.constants import AWS_SNS             # noqa: E402
from aws_lambda_fsm.constants import AWS_SQS             # noqa: E402
from aws_lambda_fsm.constants import STREAM_DATA         # noqa: E402
from aws_lambda_fsm.constants import AWS                 # noqa: E402
from aws_lambda_fsm.serialization import json_dumps_additional_kwargs  # noqa: E402
from aws_lambda_fsm.serialization import json_loads_additional_kwargs  # noqa: E402

import settings                                          # noqa: E402

random.seed(args.random_seed)
STARTED_AT = str(int(time.time()))

validate_config()


def shellquote(s):
    # https://stackoverflow.com/questions/35817/how-to-escape-os-system-calls
    return "'" + s.replace("'", "'\\''") + "'"

# setup connections to AWS
if args.run_kinesis_lambda:
    kinesis_stream_arn = getattr(settings, args.kinesis_stream_arn)
    logging.info('Kinesis stream ARN: %s', kinesis_stream_arn)
    logging.info('Kinesis endpoint: %s', settings.ENDPOINTS.get(AWS.KINESIS))
    if get_arn_from_arn_string(kinesis_stream_arn).service != AWS.KINESIS:
        logging.fatal("%s is not a Kinesis ARN", kinesis_stream_arn)
        sys.exit(1)
    kinesis_conn = get_connection(kinesis_stream_arn, disable_chaos=True)
    kinesis_stream = get_arn_from_arn_string(kinesis_stream_arn).slash_resource()
    logging.info('Kinesis stream: %s', kinesis_stream)

if args.run_sqs_lambda:
    sqs_queue_arn = getattr(settings, args.sqs_queue_arn)
    logging.info('SQS queue ARN: %s', sqs_queue_arn)
    logging.info('SQS endpoint: %s', settings.ENDPOINTS.get(AWS.SQS))
    if get_arn_from_arn_string(sqs_queue_arn).service != AWS.SQS:
        logging.fatal("%s is not a SQS ARN", sqs_queue_arn)
        sys.exit(1)
    sqs_conn = get_connection(sqs_queue_arn, disable_chaos=True)
    sqs_queue = get_arn_from_arn_string(sqs_queue_arn).colon_resource()
    sqs_queue_url = _get_sqs_queue_url(sqs_queue_arn)
    logging.info('SQS queue: %s', sqs_queue)

if args.run_dynamodb_lambda:
    dynamodb_table_arn = getattr(settings, args.dynamodb_table_arn)
    logging.info('DynamoDB table ARN: %s', dynamodb_table_arn)
    logging.info('DynamoDB endpoint: %s', settings.ENDPOINTS.get(AWS.DYNAMODB))
    if get_arn_from_arn_string(dynamodb_table_arn).service != AWS.DYNAMODB:
        logging.fatal("%s is not a DynamoDB ARN", dynamodb_table_arn)
        sys.exit(1)
    dynamodb_conn = get_connection(dynamodb_table_arn, disable_chaos=True)
    dynamodb_table = get_arn_from_arn_string(dynamodb_table_arn).slash_resource()
    logging.info('DynamoDB table: %s', dynamodb_table)

if args.run_sns_lambda:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            data_str = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(data_str, **json_loads_additional_kwargs())
            self.server.message = data[AWS_SNS.Message]
            self.send_response(200)
            self.wfile.write("")
    sns_server = http.server.HTTPServer(('', 8000), Handler)
    sns_server.message = None
    sns_topic_arn = getattr(settings, args.sns_topic_arn)
    logging.info('SNS topic ARN: %s', sns_topic_arn)
    logging.info('SNS endpoint: %s', settings.ENDPOINTS.get(AWS.SNS))
    if get_arn_from_arn_string(sns_topic_arn).service != AWS.SNS:
        logging.fatal("%s is not an SNS ARN", sns_topic_arn)
        sys.exit(1)
    sns_conn = get_connection(sns_topic_arn, disable_chaos=True)
    sns_topic = get_arn_from_arn_string(sns_topic_arn).resource
    logging.info('SNS topic: %s', sns_topic)
    response = sns_conn.subscribe(
        TopicArn=sns_topic_arn,
        Protocol='http',
        Endpoint='http://localhost:8000/'
    )

# get an iterator to the head of the stream
shard_its = []

if args.run_kinesis_lambda:
    response = kinesis_conn.describe_stream(
        StreamName=kinesis_stream,
    )
    num_shards = len(response[AWS_KINESIS.StreamDescription][AWS_KINESIS.STREAM.Shards])
    for i in range(num_shards):
        shard_id = 'shardId-%s' % str(i).zfill(12)
        shard_it = kinesis_conn.get_shard_iterator(
            StreamName=kinesis_stream,
            ShardId=shard_id,
            ShardIteratorType=AWS_KINESIS.LATEST
        )[AWS_KINESIS.ShardIterator]
        shard_its.append(shard_it)

dynamodb_old_images = {}
seen_seq_num = set()

# now loop on the stream, pulling records and calling
# the lambda handler with something approximating a lambda
# request
while True:

    lambda_context = {}

    if args.run_timer_lambda:

        # run the timer handler
        lambda_timer_handler()

    if args.run_sqs_lambda and sqs_conn:

        # receive up to 10 messages from SQS
        sqs_messages = []
        response = sqs_conn.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10
        )
        sqs_messages = response.get(AWS_SQS.Messages, [])

        if sqs_messages:

            # create the lambda event
            lambda_event = {
                AWS_LAMBDA.Records: []
            }

            # populate the lambda event
            for sqs_message in sqs_messages:
                body = sqs_message[AWS_SQS.MESSAGE.Body]
                tmp = {
                    AWS_LAMBDA.EventSource: AWS_LAMBDA.EVENT_SOURCE.SQS,
                    AWS_LAMBDA.SQS_RECORD.BODY: body
                }
                lambda_event[AWS_LAMBDA.Records].append(tmp)

            # and call the handler with the records
            if args.lambda_command:
                serialized = json.dumps(lambda_event, **json_dumps_additional_kwargs())
                quoted = shellquote(serialized)
                subprocess.call(['/bin/bash', '-c', args.lambda_command + " " + quoted])
            else:
                lambda_handler(lambda_event, lambda_context)

            # after processing, the SQS messages need to be deleted
            response = sqs_conn.delete_message_batch(
                QueueUrl=sqs_queue_url,
                Entries=[
                    {
                        AWS_SQS.MESSAGE.Id: str(i),
                        AWS_SQS.MESSAGE.ReceiptHandle: sqs_message[AWS_SQS.MESSAGE.ReceiptHandle]
                    }
                    for i, sqs_message in enumerate(sqs_messages)
                ]
            )

    if args.run_kinesis_lambda and kinesis_conn:

        # run the kinesis handler
        for i, shard_it in enumerate(shard_its):
            out = kinesis_conn.get_records(
                ShardIterator=shard_it,
                Limit=args.lambda_batch_size
            )
            shard_its[i] = out[AWS_KINESIS.NextShardIterator]

            # process any results that are available
            if out[AWS_KINESIS.Records]:

                # create the lambda event
                lambda_event = {
                    AWS_LAMBDA.Records: []
                }

                # populate the lambda event
                for record in out[AWS_KINESIS.Records]:

                    seq_num = record[AWS_KINESIS.RECORD.SequenceNumber]
                    if seq_num in seen_seq_num:
                        # kinesalite has a bug in newer versions....
                        logging.error("Skipping duplicate kinesis SequenceNumber (%s)...", seq_num)
                        continue
                    seen_seq_num.add(seq_num)

                    data = record[AWS_KINESIS.RECORD.Data]
                    tmp = {
                        AWS_LAMBDA.EventSource: AWS_LAMBDA.EVENT_SOURCE.KINESIS,
                        AWS_LAMBDA.KINESIS_RECORD.KINESIS: {
                            AWS_LAMBDA.KINESIS_RECORD.DATA: base64.b64encode(data)
                        }
                    }
                    lambda_event[AWS_LAMBDA.Records].append(tmp)

                # and call the handler with the records
                if args.lambda_command:
                    # no need to escape here since the base64 string is OK
                    subprocess.call(['/bin/bash', '-c', args.lambda_command + " '" +
                                     json.dumps(lambda_event, **json_dumps_additional_kwargs()) + "'"])
                else:
                    lambda_handler(lambda_event, lambda_context)

    if args.run_sns_lambda and sns_server:

        sns_server.handle_request()
        message = sns_server.message
        if message:
            lambda_event = {
                AWS_LAMBDA.Records: [
                    {
                        AWS_LAMBDA.EventSource: AWS_LAMBDA.EVENT_SOURCE.SNS,
                        AWS_LAMBDA.SNS_RECORD.SNS: {
                            AWS_LAMBDA.SNS_RECORD.Message: message
                        }
                    }
                ]
            }

            if args.lambda_command:
                subprocess.call(['/bin/bash', '-c', args.lambda_command + " '" +
                                 json.dumps(lambda_event, **json_dumps_additional_kwargs()) + "'"])
            else:
                lambda_handler(lambda_event, lambda_context)

    if args.run_dynamodb_lambda and dynamodb_conn:

        # run the dynamodb update handler
        scanned = dynamodb_conn.scan(
            TableName=dynamodb_table,
            ConsistentRead=True,
            ScanFilter={
                STREAM_DATA.TIMESTAMP: {
                    AWS_DYNAMODB.ComparisonOperator: AWS_DYNAMODB.GREATER_THAN,
                    AWS_DYNAMODB.AttributeValueList: [{AWS_DYNAMODB.NUMBER: STARTED_AT}]
                },
            }
        )

        if scanned[AWS_DYNAMODB.Items]:

            # create the lambda event
            lambda_event = {
                AWS_LAMBDA.Records: []
            }

            # populate the lambda event
            for record in scanned[AWS_DYNAMODB.Items]:
                correlation_id = record[STREAM_DATA.CORRELATION_ID][AWS_DYNAMODB.STRING]
                payload = record[STREAM_DATA.PAYLOAD][AWS_DYNAMODB.STRING]
                timestamp = record[STREAM_DATA.TIMESTAMP][AWS_DYNAMODB.NUMBER]
                create = correlation_id not in dynamodb_old_images
                update = correlation_id in dynamodb_old_images and payload != dynamodb_old_images[correlation_id]
                if create or update:
                    # this is a CREATE or UPDATE
                    tmp = {
                        AWS_LAMBDA.EventSource: AWS_LAMBDA.EVENT_SOURCE.DYNAMODB,
                        AWS_LAMBDA.DYNAMODB_RECORD.DYNAMODB: {
                            AWS_LAMBDA.DYNAMODB_RECORD.NewImage: {
                                STREAM_DATA.PAYLOAD: {
                                    AWS_DYNAMODB.STRING: payload
                                }
                            }
                        }
                    }
                    lambda_event[AWS_LAMBDA.Records].append(tmp)
                    dynamodb_old_images[correlation_id] = payload

            # and call the handler with the records
            if lambda_event[AWS_LAMBDA.Records]:

                if args.lambda_command:
                    subprocess.call(['/bin/bash', '-c', args.lambda_command + " '" +
                                     json.dumps(lambda_event, **json_dumps_additional_kwargs()) + "'"])
                else:
                    lambda_handler(lambda_event, lambda_context)

    time.sleep(args.sleep_time)
