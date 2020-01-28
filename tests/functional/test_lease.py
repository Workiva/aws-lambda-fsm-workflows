from __future__ import absolute_import
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
from builtins import object
import uuid
import time

# library imports
from nose.plugins.attrib import attr
import memcache
import mock
import redis

# application imports
from aws_lambda_fsm import aws
from .test_base import MemcachedTest
from .test_base import RedisTest
from .test_base import DynamodbTest


class SmokeTest(object):

    def smoke(self):
        correlation_id = uuid.uuid4().hex

        # can acquire lease
        acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
        self.assertTrue(acquired is 1)

        # cannot re-acquire own lease (not re-entrant)
        acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
        self.assertTrue(acquired is False)

        # cannot acquire someone else's lease
        acquired = aws.acquire_lease(correlation_id, 1, 2, primary=True)
        self.assertTrue(acquired is False)

        # cannot release someone else's lease
        released = aws.release_lease(correlation_id, 1, 2, 1, primary=True)
        self.assertTrue(released is False)

        # cannot release own lease (different fence token)
        released = aws.release_lease(correlation_id, 1, 1, 99, primary=True)
        self.assertTrue(released is False)

        # can release own lease
        released = aws.release_lease(correlation_id, 1, 1, 1, primary=True)
        self.assertTrue(released is True)

        # someone else can acquire new lease with short timeout
        acquired = aws.acquire_lease(correlation_id, 1, 2, primary=True, timeout=1)
        self.assertTrue(acquired is 2)

        time.sleep(2)

        # someone else can acquire new lease when previous times out
        acquired = aws.acquire_lease(correlation_id, 1, 2, primary=True)
        self.assertTrue(acquired is 3)


@attr('functional')
class MemcachedSmokeTest(MemcachedTest, SmokeTest):

    def test_smoke(self):
        self.smoke()

    @mock.patch('aws_lambda_fsm.aws.logger')
    def test_add_collision_when_missing(self, mock_logger):
        correlation_id = uuid.uuid4().hex

        # init aws memcache connection
        connection = aws.get_connection(self.mock_settings.PRIMARY_CACHE_SOURCE)

        def inline(x, c, ac):
            y = c.original_gets(x)
            ac.add('lease-' + correlation_id, '99:99:99:99')
            return y

        try:
            connection.original_gets = connection.gets
            another_connection = memcache.Client(['localhost:11211'], cache_cas=True)
            connection.gets = lambda x: inline(x, connection, another_connection)
            acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
            self.assertFalse(acquired)
            self.assertEqual(
                mock.call.warn("Cannot acquire memcache lease: unexpectedly lost 'memcache.add' race"),
                mock_logger.mock_calls[-1]
            )

        finally:
            connection.gets = connection.original_gets
            del connection.original_gets

    @mock.patch('aws_lambda_fsm.aws.logger')
    def test_cas_collision_when_expired(self, mock_logger):
        correlation_id = uuid.uuid4().hex

        # init aws memcache connection
        connection = aws.get_connection(self.mock_settings.PRIMARY_CACHE_SOURCE)
        connection.set('lease-' + correlation_id, '-1:-1:0:99')

        def inline(x, c, ac):
            y = c.original_gets(x)
            ac.gets('lease-' + correlation_id)
            ac.cas('lease-' + correlation_id, '99:99:99:99')
            return y

        try:
            connection.original_gets = connection.gets
            another_connection = memcache.Client(['localhost:11211'], cache_cas=True)
            connection.gets = lambda x: inline(x, connection, another_connection)
            acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
            self.assertFalse(acquired)
            self.assertEqual(
                mock.call.warn("Cannot acquire memcache lease: unexpectedly lost 'memcache.cas' race"),
                mock_logger.mock_calls[-1]
            )

        finally:
            connection.gets = connection.original_gets
            del connection.original_gets


@attr('functional')
class RedisSmokeTest(RedisTest, SmokeTest):

    def test_smoke(self):
        self.smoke()

    @mock.patch('aws_lambda_fsm.aws.logger')
    def test_watch_collision_when_missing(self, mock_logger):
        correlation_id = uuid.uuid4().hex

        # init aws redis connection
        aws.get_connection(self.mock_settings.PRIMARY_CACHE_SOURCE)

        def inline(a, b, c, d, ac):
            r = aws.original_serialize_lease_value(a, b, c, d)
            ac.setex('lease-' + correlation_id, 3600, '99:99:99:99')
            return r

        try:
            aws.original_serialize_lease_value = aws._serialize_lease_value
            another_connection = redis.StrictRedis(host='localhost', port=6379, db=0)
            aws._serialize_lease_value = lambda a, b, c, d: inline(a, b, c, d, another_connection)
            acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
            self.assertFalse(acquired)
            self.assertEqual(
                mock.call.warn("Cannot acquire redis lease: unexpectedly lost 'pipe.watch' race"),
                mock_logger.mock_calls[-1]
            )

        finally:
            aws._serialize_lease_value = aws._serialize_lease_value
            del aws.original_serialize_lease_value

    @mock.patch('aws_lambda_fsm.aws.logger')
    def test_watch_collision_when_expired(self, mock_logger):
        correlation_id = uuid.uuid4().hex

        # init aws redis connection
        connection = aws.get_connection(self.mock_settings.PRIMARY_CACHE_SOURCE)
        connection.setex('lease-' + correlation_id, 3600, '-1:-1:0:99')

        def inline(x, c, ac):
            r = aws.original_deserialize_lease_value(x)
            ac.setex('lease-' + correlation_id, 3600, '99:99:99:99')
            return r

        try:
            aws.original_deserialize_lease_value = aws._deserialize_lease_value
            another_connection = redis.StrictRedis(host='localhost', port=6379, db=0)
            aws._deserialize_lease_value = lambda x: inline(x, connection, another_connection)
            acquired = aws.acquire_lease(correlation_id, 1, 1, primary=True)
            self.assertFalse(acquired)
            self.assertEqual(
                mock.call.warn("Cannot acquire redis lease: unexpectedly lost 'pipe.watch' race"),
                mock_logger.mock_calls[-1]
            )

        finally:
            aws._deserialize_lease_value = aws._deserialize_lease_value
            del aws.original_deserialize_lease_value


@attr('functional')
class DynamodbSmokeTest(DynamodbTest, SmokeTest):

    def test_smoke(self):
        self.smoke()
