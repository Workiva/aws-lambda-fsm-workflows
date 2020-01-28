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

from builtins import object
import logging

logging.basicConfig()
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger().setLevel(logging.DEBUG)

from aws_lambda_fsm.config import set_settings  # flake8: noqa


class TestSettings(object):
    """
    Settings for unit tests.
    """
    USE_KINESIS = True
    USE_DYNAMODB = True
    USE_SNS = True
    USE_ECS = True
    USE_CLOUDWATCH = True
    USE_ELASTICACHE = True
    PRIMARY_METRICS_SOURCE = 'arn:partition:cloudwatch:testing:account:resource'
    SECONDARY_METRICS_SOURCE = None
    PRIMARY_CACHE_SOURCE = 'arn:partition:elasticache:testing:account:resource'
    SECONDARY_CACHE_SOURCE = None
    PRIMARY_STREAM_SOURCE = 'arn:partition:kinesis:testing:account:stream/resource'
    SECONDARY_STREAM_SOURCE = None
    PRIMARY_CHECKPOINT_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_CHECKPOINT_SOURCE = None
    PRIMARY_RETRY_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_RETRY_SOURCE = None
    PRIMARY_ENVIRONMENT_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_ENVIRONMENT_SOURCE = None
    AWS_CHAOS = {}
    ENDPOINTS = {
        'kinesis': {
            'testing': 'invalid_endpoint'
        },
        'dynamodb': {
            'testing': 'invalid_endpoint'
        },
        'elasticache': {
            'testing': 'invalid_endpoint:9999'
        },
        'sns': {
            'testing': 'invalid_endpoint'
        },
        'cloudwatch': {
            'testing': 'invalid_endpoint'
        }
    }
    BOTO3_CLIENT_ADDITIONAL_KWARGS = {}

set_settings(TestSettings)
