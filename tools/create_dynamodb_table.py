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
# create_dynamodb_table.py
#
# Script that creates a Dynamodb for use by state machines.

# system imports
import argparse
import logging
import sys

# library imports

# application imports
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.aws import validate_config
from aws_lambda_fsm.constants import ENVIRONMENT_DATA
from aws_lambda_fsm.constants import RETRY_DATA
from aws_lambda_fsm.constants import CHECKPOINT_DATA
from aws_lambda_fsm.constants import CACHE_DATA
from aws_lambda_fsm.constants import STREAM_DATA
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.constants import AWS

import settings

# setup the command line args
parser = argparse.ArgumentParser(description='Creates AWS DynamoDB tables.')
parser.add_argument('--dynamodb_table_arn', default='PRIMARY_STREAM_SOURCE')
parser.add_argument('--dynamodb_read_capacity_units', type=int, default=10)
parser.add_argument('--dynamodb_write_capacity_units', type=int, default=10)
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
dynamodb_table_arn = getattr(settings, args.dynamodb_table_arn)
logging.info('DynamoDB table ARN: %s', dynamodb_table_arn)
logging.info('DynamoDB endpoint: %s', settings.ENDPOINTS.get(AWS.DYNAMODB))
if get_arn_from_arn_string(dynamodb_table_arn).service != AWS.DYNAMODB:
    logging.fatal("%s is not a DynamoDB ARN", dynamodb_table_arn)
    sys.exit(1)
dynamodb_conn = get_connection(dynamodb_table_arn, disable_chaos=True)
dynamodb_table = get_arn_from_arn_string(dynamodb_table_arn).slash_resource()
logging.info('DynamoDB table: %s', dynamodb_table)

if 'RESULTS' in args.dynamodb_table_arn:
    # create a dynamodb table for examples/tracer
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: 'correlation_id',
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            },
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: 'correlation_id',
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)

if 'CHECKPOINT' in args.dynamodb_table_arn:
    # create a dynamodb table for checkpoints
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: CHECKPOINT_DATA.CORRELATION_ID,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            },
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: CHECKPOINT_DATA.CORRELATION_ID,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)

if 'STREAM' in args.dynamodb_table_arn:
    # create a dynamodb table for streaming events
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: STREAM_DATA.CORRELATION_ID,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            },
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: STREAM_DATA.CORRELATION_ID,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)

if 'RETRY' in args.dynamodb_table_arn:
    # create a dynamodb table for retries. we use partition as the key hash because
    # we want to make the queries for retry entities consistent, and we can easily
    # iterate over a known list (0,...,15) or partitions.
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: RETRY_DATA.PARTITION,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.NUMBER
            },
            {
                AWS_DYNAMODB.AttributeName: RETRY_DATA.CORRELATION_ID_STEPS,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            },
            {
                AWS_DYNAMODB.AttributeName: RETRY_DATA.RUN_AT,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.NUMBER
            },
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: RETRY_DATA.PARTITION,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            },
            {
                AWS_DYNAMODB.AttributeName: RETRY_DATA.CORRELATION_ID_STEPS,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.RANGE
            },
        ],
        LocalSecondaryIndexes=[
            {
                AWS_DYNAMODB.IndexName: RETRY_DATA.RETRIES,
                AWS_DYNAMODB.KeySchema: [
                    {
                        AWS_DYNAMODB.AttributeName: RETRY_DATA.PARTITION,
                        AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
                    },
                    {
                        AWS_DYNAMODB.AttributeName: RETRY_DATA.RUN_AT,
                        AWS_DYNAMODB.KeyType: AWS_DYNAMODB.RANGE
                    }
                ],
                AWS_DYNAMODB.Projection: {
                    AWS_DYNAMODB.ProjectionType: AWS_DYNAMODB.ALL
                }
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)

if 'ENVIRONMENT' in args.dynamodb_table_arn:
    # create a dynamodb table storing task environments in order to get around the
    # 8192 character limit
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: ENVIRONMENT_DATA.GUID,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            }
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: ENVIRONMENT_DATA.GUID,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)

if 'CACHE' in args.dynamodb_table_arn:
    # create a cache table
    response = dynamodb_conn.create_table(
        TableName=dynamodb_table,
        AttributeDefinitions=[
            {
                AWS_DYNAMODB.AttributeName: CACHE_DATA.KEY,
                AWS_DYNAMODB.AttributeType: AWS_DYNAMODB.STRING
            }
        ],
        KeySchema=[
            {
                AWS_DYNAMODB.AttributeName: CACHE_DATA.KEY,
                AWS_DYNAMODB.KeyType: AWS_DYNAMODB.HASH
            }
        ],
        ProvisionedThroughput={
            AWS_DYNAMODB.ReadCapacityUnits: args.dynamodb_read_capacity_units,
            AWS_DYNAMODB.WriteCapacityUnites: args.dynamodb_write_capacity_units
        }
    )
    logging.info(response)
