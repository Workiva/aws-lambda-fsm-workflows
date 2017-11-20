# Copyright 2016 Workiva Inc.
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

import unittest
import mock
import base64
import json

from botocore.exceptions import ClientError

from aws_lambda_fsm.client import start_state_machine
from aws_lambda_fsm.fsm import FSM
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm import handler


class Messages(object):

    def __init__(self):
        self.messages = []
        self.all_messages = []

    def send(self, data):
        self.messages.append(data)
        self.all_messages.append(data)

    def recv(self):
        return self.messages.pop(0) if self.messages else None

    def reset(self):
        del self.messages[:]
        del self.all_messages[:]

    def trace(self, uvars=()):
        s = 'system_context'
        u = 'user_context'
        svars = ('current_state', 'current_event', 'steps', 'retries')
        data = enumerate(map(lambda x: json.loads(x), self.all_messages))
        return map(
            lambda x: (
                x[0],
                tuple(x[1][s][v] for v in svars),
                tuple(x[1][u].get(v) for v in uvars)
            ),
            data
        )


# not threadsafe, yada yada
class AWSStub(object):

    def __init__(self):
        self.primary_stream_source = Messages()
        self.secondary_stream_source = Messages()
        self.primary_stream_source_failure = False

        self.primary_retry_source = Messages()
        self.secondary_retry_source = Messages()
        self.primary_retry_source_failure = False

        self.all_sources = Messages()

        self.primary_cache = {}
        self.secondary_cache = {}

    def _get_stream_source(self, primary):
        if self.primary_stream_source_failure and primary:
            raise ClientError({"Error": {}}, "mock")
        return {
            True: self.primary_stream_source,
            False: self.secondary_stream_source
        }[primary]

    def _get_cache_source(self, primary):
        return {
            True: self.primary_cache,
            False: self.secondary_cache
        }[primary]

    def _get_retry_source(self, primary):
        return {
            True: self.primary_retry_source,
            False: self.secondary_retry_source
        }[primary]

    def get_message(self):
        return AWS.primary_stream_source.recv() or \
            AWS.secondary_stream_source.recv() or \
            AWS.primary_retry_source.recv() or \
            AWS.secondary_retry_source.recv()

    def send_next_event_for_dispatch(self, context, data, correlation_id, delay=0, primary=True):
        self._get_stream_source(primary).send(data)
        self.all_sources.send(data)
        return {'test': 'stub'}

    def set_message_dispatched(self, correlation_id, steps, retries, primary=True):
        self._get_cache_source(primary)['%s-%s' % (correlation_id, steps)] = True
        return True

    def get_message_dispatched(self, correlation_id, steps, primary=True):
        self._get_cache_source(primary).get('%s-%s' % (correlation_id, steps))

    def acquire_lease(self, correlation_id, steps, retries, primary=True):
        key = 'lease-%s-%s' % (correlation_id, steps)
        if key in self._get_cache_source(primary):
            return False
        else:
            self._get_cache_source(primary)[key] = True
            return True

    def release_lease(self, correlation_id, steps, retries, fence_token, primary=True):
        key = 'lease-%s-%s' % (correlation_id, steps)
        if key in self._get_cache_source(primary):
            self._get_cache_source(primary).pop(key)
            return True
        else:
            return False

    def start_retries(self, context, run_at, payload, primary=True):
        self._get_retry_source(primary).send(payload)
        self.all_sources.send(payload)
        return {'test': 'stub'}

    def increment_error_counters(self, data, dimensions):
        return {'test': 'stub'}

    def store_checkpoint(self, context, sent, primary=True):
        return {'test': 'stub'}

    def reset(self):
        self.primary_stream_source.reset()
        self.secondary_stream_source.reset()
        self.primary_retry_source.reset()
        self.secondary_retry_source.reset()
        self.all_sources.reset()
        self.primary_cache.clear()
        self.secondary_cache.clear()
        self.primary_stream_source_failure = False


def to_kinesis_message(data):
    return {
        "Records": [{
            "kinesis": {
                "data": base64.b64encode(data)
            }
        }]
    }


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
class Test(unittest.TestCase):

    def setUp(self):
        AWS.reset()
        FSM(get_current_configuration('tests/functional/fsm.yaml'))

    def _execute(self, machine_name, context, fail_primary=False):
        start_state_machine(machine_name, context)
        if fail_primary:
            AWS.primary_stream_source_failure = True
        message = AWS.get_message()
        while message:
            handler.lambda_kinesis_handler(to_kinesis_message(message))
            message = AWS.get_message()

    def test_simple(self, *args):
        self._execute("simple", {})
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())

    def test_simple_with_primary_failure(self, *args):
        self._execute("simple", {}, fail_primary=True)
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

    def test_simple_with_failure(self, *args):
        self._execute("simple", {'fail_at': [(0, 0)]})
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), ()),
            (1, ('pseudo_init', 'pseudo_init', 0, 1), ()),  # retry
            (2, ('start', 'ok', 1, 0), ())
        ]
        self.assertEqual(expected, AWS.all_sources.trace())

    def test_looper(self, *args):
        self._execute("looper", {"loops": 3})
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 2, 0), (2,)),
            (3, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))

    def test_looper_with_primary_failure(self, *args):
        self._execute("looper", {"loops": 3}, fail_primary=True)
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

    def test_looper_with_failure(self, *args):
        self._execute("looper", {"loops": 3, 'fail_at': [(1, 0)]})
        expected = [
            (0, ('pseudo_init', 'pseudo_init', 0, 0), (None,)),
            (1, ('start', 'ok', 1, 0), (1,)),
            (2, ('start', 'ok', 1, 1), (1,)),  # retry
            (3, ('start', 'ok', 2, 0), (2,)),
            (4, ('start', 'done', 3, 0), (3,))
        ]
        self.assertEqual(expected, AWS.all_sources.trace(('counter',)))