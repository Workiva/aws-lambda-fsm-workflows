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

# system imports
import unittest
import logging

# library imports
import mock

# application imports
from aws_lambda_fsm import aws
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.constants import CACHE_DATA

logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger().setLevel(0)


MEMCACHED_CACHE_SOURCE = 'arn:partition:elasticache:testing:account:primary_cache_source'
REDIS_CACHE_SOURCE = 'arn:partition:elasticache:testing:account:secondary_cache_source'
DYNAMODB_CACHE_SOURCE = 'arn:partition:dynamodb:testing:account:table/cache_source'
ENDPOINTS = {
    DYNAMODB_CACHE_SOURCE: 'http://localhost:4569'
}
ELASTICACHE_ENDPOINTS = {
    MEMCACHED_CACHE_SOURCE: {
        'CacheClusterId': 'memcached-cluster',
        'Engine': 'memcached',
        'ConfigurationEndpoint': {
            'Address': 'localhost',
            'Port': 11211
        }
    },
    REDIS_CACHE_SOURCE: {
        'CacheClusterId': 'redis-cluster',
        'Engine': 'redis',
        'CacheNodes': [
            {
                'Endpoint': {
                    'Address': 'localhost',
                    'Port': 6379
                }
            }
        ]
    }
}


class MockSettingsTest(unittest.TestCase):

    def setUp(self):
        self.patcher = mock.patch('aws_lambda_fsm.aws.settings')
        self.addCleanup(self.patcher.stop)
        self.mock_settings = self.patcher.start()
        self.mock_settings.AWS_CHAOS = None
        self.patch_settings()

    def patch_settings(self):
        raise NotImplementedError()


class MemcachedTest(MockSettingsTest):

    def patch_settings(self):
        self.mock_settings.PRIMARY_CACHE_SOURCE = MEMCACHED_CACHE_SOURCE
        self.mock_settings.ENDPOINTS = ENDPOINTS
        self.mock_settings.ELASTICACHE_ENDPOINTS = ELASTICACHE_ENDPOINTS


class RedisTest(MockSettingsTest):

    def patch_settings(self):
        self.mock_settings.PRIMARY_CACHE_SOURCE = REDIS_CACHE_SOURCE
        self.mock_settings.ENDPOINTS = ENDPOINTS
        self.mock_settings.ELASTICACHE_ENDPOINTS = ELASTICACHE_ENDPOINTS


class DynamodbTest(MockSettingsTest):

    def setUp(self):
        super(DynamodbTest, self).setUp()
        self.create_cache()

    def patch_settings(self):
        self.mock_settings.PRIMARY_CACHE_SOURCE = DYNAMODB_CACHE_SOURCE
        self.mock_settings.ENDPOINTS = ENDPOINTS
        self.mock_settings.ELASTICACHE_ENDPOINTS = ELASTICACHE_ENDPOINTS

    def create_cache(self):
        try:
            dynamodb_conn = aws.get_connection(DYNAMODB_CACHE_SOURCE)
            dynamodb_table = aws.get_arn_from_arn_string(DYNAMODB_CACHE_SOURCE).slash_resource()
            dynamodb_conn.create_table(
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
                    AWS_DYNAMODB.ReadCapacityUnits: 10,
                    AWS_DYNAMODB.WriteCapacityUnites: 10
                }
            )
        except:
            pass
