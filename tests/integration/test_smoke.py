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
import threading
import time

# library imports

# application imports
from aws_lambda_fsm.fsm import FSM
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm.constants import AWS as AWS_CONSTANTS
from tests.integration.utils import AWSStub
from tests.integration.utils import BaseFunctionalTest
from .actions import get_counter
from .actions import set_counter

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
        set_counter(0)
        self.assertEqual(0, get_counter())
        self._execute(AWS, "looper", {"loops": 3})
        self.assertEqual(3, get_counter())

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
    # START: machine_name="looper-local"
    ################################################################################

    def test_looper_local(self, *args):
        set_counter(0)
        self.assertEqual(0, get_counter())

        AWS.add_callback('send_next_event_for_dispatch', mock.Mock(side_effect=([None] + [Exception()] * 100)))
        self._execute(AWS, "looper-local", {"loops": 3})
        self.assertEqual(3, get_counter())

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'lease-correlation_id-0': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = []
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_looper_local_with_failure(self, *args):
        set_counter(0)
        self.assertEqual(0, get_counter())

        AWS.add_callback('send_next_event_for_dispatch', mock.Mock(side_effect=([None] + [Exception()] * 100)))
        self._execute(AWS, "looper-local", {"loops": 3, 'fail_at': [(i, 0) for i in range(100)]})
        self.assertEqual(3, get_counter())

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('pseudo_init', 'pseudo_init', 0, 1), (None,))  # retry
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))

        # check cache
        expected = {
            'correlation_id-0': True
        }
        self.assertEqual(expected, AWS.primary_cache)
        self.assertEqual(expected, AWS.secondary_cache)
        expected = {
            'correlation_id-0': True,
            'lease-correlation_id-0': True
        }
        self.assertEqual(expected, AWS.all_caches)

        # check errors
        expected = [
            [
                {'retry': 1},
                {'current_state': 'pseudo_init', 'current_event': 'pseudo_init', 'machine_name': 'looper-local'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    ################################################################################
    # START: machine_name="looper-mixed"
    ################################################################################

    def test_looper_mixed(self, *args):
        set_counter(0)
        self.assertEqual(0, get_counter())
        self._execute(AWS, "looper-mixed", {"loops": 3})
        self.assertEqual(6, get_counter())

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'done', 1, 0), (3,)),
            (2, ('reset', 'done', 2, 0), (None,)),
            (3, ('loop', 'done', 3, 0), (3,))
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

    def test_looper_mixed_with_failure(self, *args):
        set_counter(0)
        self.assertEqual(0, get_counter())
        self._execute(AWS, "looper-mixed", {"loops": 3, 'fail_at': [(i, 0) for i in range(100)]})
        self.assertEqual(6, get_counter())

        # check messages
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('pseudo_init', 'pseudo_init', 0, 1), (None,)),  # retry
            (2, ('start', 'done', 1, 0), (3,)),
            (3, ('reset', 'done', 2, 0), (None,)),
            (4, ('reset', 'done', 2, 1), (None,)),  # retry
            (5, ('loop', 'done', 3, 0), (3,))
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
                {'current_event': 'pseudo_init', 'current_state': 'pseudo_init', 'machine_name': 'looper-mixed'}
            ],
            [
                {'retry': 1},
                {'current_event': 'done', 'current_state': 'reset', 'machine_name': 'looper-mixed'}
            ]
        ]
        self.assertEqual(expected, AWS.errors.trace(raw=True))

    def test_looper_mixed_uses_queue(self, *args):
        AWS.add_callback('send_next_event_for_dispatch', mock.Mock(side_effect=Exception()))
        self.assertRaises(Exception, self._execute, AWS, "looper-mixed", {"loops": 3})

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

    ################################################################################
    # START: machine_name="longpause"
    ################################################################################

    def test_two_at_same_time(self, *args):
        thread1 = TestThread(self, "longpause", {'key': 'val1'})
        thread2 = TestThread(self, "longpause", {'key': 'val2'})
        thread1.start()
        time.sleep(2)
        thread2.start()
        thread1.join()
        thread2.join()
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ('val1',)),
            (1, ('pseudo_init', 'pseudo_init', 0, 0), ('val2',)),  # both start
            (2, ('pseudo_init', 'pseudo_init', 0, 1), ('val2',)),  # second unable to acquire lease
            (3, ('start', 'ok', 1, 0), ('val1',)),
            (4, ('middle', 'ok', 2, 0), ('val1',)),  # first finished
            (5, ('start', 'ok', 1, 0), ('val2',))  # second gets lease, but that has already run
        ]
        self.assertEqual(expected, AWS.all_sources.trace(uvars={"key"}))


class TestThread(threading.Thread):

    def __init__(self, test, name, context):
        threading.Thread.__init__(self)
        self.test = test
        self.name = name
        self.context = context

    def run(self):
        self.test._execute(AWS, self.name, self.context)


class TestSqs(Test):

    MESSAGE_TYPE = AWS_CONSTANTS.SQS


class TestSns(Test):

    MESSAGE_TYPE = AWS_CONSTANTS.SNS
