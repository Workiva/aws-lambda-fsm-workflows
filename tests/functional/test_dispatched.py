# system imports
import uuid
import time

# library imports
from nose.plugins.attrib import attr

# application imports
from aws_lambda_fsm import aws
from test_base import MemcachedTest
from test_base import RedisTest
from test_base import DynamodbTest


class SmokeTest(object):

    def smoke(self):
        correlation_id = uuid.uuid4().hex

        # can dispatch
        dispatched = aws.set_message_dispatched(correlation_id, 1, 1)
        self.assertTrue(dispatched is True)

        # is dispatched
        dispatched = aws.get_message_dispatched(correlation_id, 1)
        self.assertTrue(dispatched == correlation_id + '-1-1')

        # can dispatch with timeout
        dispatched = aws.set_message_dispatched(correlation_id, 1, 1, timeout=1)
        self.assertTrue(dispatched is True)

        time.sleep(2)

        # is not dispatched after timeout
        dispatched = aws.get_message_dispatched(correlation_id, 1)
        self.assertTrue(dispatched is None)


@attr('functional')
class MemcachedSmokeTest(MemcachedTest, SmokeTest):

    def test_smoke(self):
        self.smoke()


@attr('functional')
class RedisSmokeTest(RedisTest, SmokeTest):

    def test_smoke(self):
        self.smoke()


@attr('functional')
class DynamodbSmokeTest(DynamodbTest, SmokeTest):

    def test_smoke(self):
        self.smoke()
