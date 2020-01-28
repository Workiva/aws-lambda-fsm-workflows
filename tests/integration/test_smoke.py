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
import mock

# library imports

# application imports
from aws_lambda_fsm.fsm import FSM
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm.constants import AWS as AWS_CONSTANTS
from tests.integration.utils import AWSStub
from tests.integration.utils import BaseFunctionalTest

AWS = AWSStub()


@mock.patch("aws_lambda_fsm.client.send_next_event_for_dispatch", wraps=AWS.send_next_event_for_dispatch)
@mock.patch("aws_lambda_fsm.fsm.send_next_event_for_dispatch", wraps=AWS.send_next_event_for_dispatch)
@mock.patch("aws_lambda_fsm.fsm.set_message_dispatched", wraps=AWS.set_message_dispatched)
@mock.patch("aws_lambda_fsm.fsm.get_message_dispatched", wraps=AWS.get_message_dispatched)
@mock.patch("aws_lambda_fsm.fsm.acquire_lease", wraps=AWS.acquire_lease)
@mock.patch("aws_lambda_fsm.fsm.release_lease", wraps=AWS.release_lease)
@mock.patch("aws_lambda_fsm.fsm.start_retries", wraps=AWS.start_retries)
@mock.patch("aws_lambda_fsm.fsm.increment_error_counters", wraps=AWS.increment_error_counters)
@mock.patch("aws_lambda_fsm.fsm.store_checkpoint", wraps=AWS.store_checkpoint)
class Test(BaseFunctionalTest):

    def setUp(self):
        AWS.reset()
        FSM(get_current_configuration('tests/integration/fsm.yaml'))

    ################################################################################
    # START: machine_name="simple"
    ################################################################################

    def test_simple(self, *args):
        self._execute(AWS, "simple", {})

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = []
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_simple_with_primary_failure(self, *args):
        self._execute(AWS, "simple", {}, primary_stream_chaos=1.0)

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
        ]
        self.assertEqual(expected, AWS.primary_stream_source.trace())
        expected = [
            (0, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.secondary_stream_source.trace())

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [{'error': 1}, {'current_state': 'pseudo_init', 'current_event': 'pseudo_init', 'machine_name': 'simple'}]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_simple_with_failure(self, *args):
        self._execute(AWS, "simple", {'fail_at': [(0, 0)]})

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('pseudo_init', 'pseudo_init', 0, 1), ()),  # retry
            (2, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True
        }
        self.assertEqual(expected, AWS.primary_cache)

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'retry': 1},
                {'current_state': 'pseudo_init', 'current_event': 'pseudo_init', 'machine_name': 'simple'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_simple_with_failure_with_primary_retry_failure(self, *args):
        self._execute(AWS, "simple", {'fail_at': [(0, 0)]}, primary_retry_chaos=1.0)

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('pseudo_init', 'pseudo_init', 0, 1), ()),  # retry
            (2, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())
        expected = []
        self.assertEqual(expected, AWS.primary_retry_source.trace())
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 1), ())
        ]
        self.assertEqual(expected, AWS.secondary_retry_source.trace())

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'retry': 1, 'error': 1},
                {'current_state': 'pseudo_init', 'current_event': 'pseudo_init', 'machine_name': 'simple'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    ################################################################################
    # START: machine_name="looper"
    ################################################################################

    def test_looper(self, *args):
        self._execute(AWS, "looper", {"loops": 3})

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 2, 0), (2,)),
            (3, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
            'lease-correlation_id-2': True,
            'lease-correlation_id-3': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = []
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_looper_with_primary_failure(self, *args):
        self._execute(AWS, "looper", {"loops": 3}, primary_stream_chaos=1.0)

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 2, 0), (2,)),
            (3, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
        ]
        self.assertEqual(expected, AWS.primary_stream_source.trace(('counter',)))
        expected = [
            (0, ('start', 'ok', 1, 0), (1,)),
            (1, ('start', 'ok', 2, 0), (2,)),
            (2, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.secondary_stream_source.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
            'lease-correlation_id-2': True,
            'lease-correlation_id-3': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'error': 1}, {'current_state': 'pseudo_init', 'current_event': 'pseudo_init', 'machine_name': 'looper'}
            ],
            [
                {'error': 1}, {'current_state': 'start', 'current_event': 'ok', 'machine_name': 'looper'}
            ],
            [
                {'error': 1}, {'current_state': 'start', 'current_event': 'ok', 'machine_name': 'looper'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_looper_with_failure(self, *args):
        self._execute(AWS, "looper", {"loops": 3, 'fail_at': [(1, 0)]})

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 1, 1), (1,)),  # retry
            (3, ('start', 'ok', 2, 0), (2,)),
            (4, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
            'lease-correlation_id-2': True,
            'lease-correlation_id-3': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'retry': 1},
                {'current_state': 'start', 'current_event': 'ok', 'machine_name': 'looper'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_looper_with_failure_with_primary_retry_failure(self, *args):
        self._execute(AWS, "looper", {"loops": 3, 'fail_at': [(1, 0)]}, primary_retry_chaos=1.0)

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 1, 1), (1,)),  # retry
            (3, ('start', 'ok', 2, 0), (2,)),
            (4, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))
        expected = []
        self.assertEqual(expected, AWS.primary_retry_source.trace(('counter',)))
        expected = [
            (0, ('start', 'ok', 1, 1), (1,))
        ]
        self.assertEqual(expected, AWS.secondary_retry_source.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'correlation_id-1': True,
            'correlation_id-2': True,
            'correlation_id-3': True,
            'lease-correlation_id-0': True,
            'lease-correlation_id-1': True,
            'lease-correlation_id-2': True,
            'lease-correlation_id-3': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'retry': 1, 'error': 1},
                {'current_state': 'start', 'current_event': 'ok', 'machine_name': 'looper'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    ################################################################################
    # START: machine_name="serialization"
    ################################################################################

    def test_serialization(self, *args):
        self._execute(AWS, "serialization", {})

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), ('<not_serializable>',)),
            (2, ('middle', 'ok', 2, 0), ('<not_serializable>',))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(uvars={"error"}))


class TestSqs(Test):

    MESSAGE_TYPE = AWS_CONSTANTS.SQS


class TestSns(Test):

    MESSAGE_TYPE = AWS_CONSTANTS.SNS
