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


@attr('functional')
class RedisSmokeTest(RedisTest, SmokeTest):

    def test_smoke(self):
        self.smoke()


@attr('functional')
class DynamodbSmokeTest(DynamodbTest, SmokeTest):

    def test_smoke(self):
        self.smoke()
