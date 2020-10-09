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
import base64
import json
import unittest
import random

# library imports
from botocore.exceptions import ClientError

# application imports
from aws_lambda_fsm.client import start_state_machine
from aws_lambda_fsm import handler
from aws_lambda_fsm.constants import AWS as AWS_CONSTANTS
from aws_lambda_fsm.serialization import json_dumps_additional_kwargs
from aws_lambda_fsm.serialization import json_loads_additional_kwargs


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

    def trace(self, uvars=(), raw=False):
        s = 'system_context'
        u = 'user_context'
        svars = ('current_state', 'current_event', 'steps', 'retries')
        serialized = [json.loads(x, **json_loads_additional_kwargs()) for x in self.all_messages]
        if raw:
            return serialized
        data = enumerate(serialized)
        return [(
            x[0],
            tuple(x[1][s][v] for v in svars),
            tuple(x[1][u].get(v) for v in uvars)
        ) for x in data]


# not threadsafe, yada yada
class AWSStub(object):

    def __init__(self):
        self.primary_stream_source = Messages()
        self.secondary_stream_source = Messages()
        self.primary_retry_source = Messages()
        self.secondary_retry_source = Messages()
        self.all_sources = Messages()

        self.primary_cache = {}
        self.secondary_cache = {}
        self.all_caches = {}
        self.empty_primary_cache = False
        self.empty_secondary_cache = False

        self.primary_stream_chaos = 0.0
        self.secondary_stream_chaos = 0.0
        self.primary_retry_chaos = 0.0
        self.secondary_retry_chaos = 0.0
        self.primary_cache_chaos = 0.0
        self.secondary_cache_chaos = 0.0

        self.errors = Messages()
        self.callbacks = {}

    def add_callback(self, method, callback):
        self.callbacks[method] = callback

    def _get_stream_source(self, primary):
        chaos = {True: self.primary_stream_chaos, False: self.secondary_stream_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            raise ClientError({"Error": {}}, "chaos")
        return {
            True: self.primary_stream_source,
            False: self.secondary_stream_source
        }[primary]

    def _get_cache_source(self, primary):
        if self.empty_primary_cache:
            self.primary_cache.clear()
        if self.empty_secondary_cache:
            self.secondary_cache.clear()
        return {
            True: self.primary_cache,
            False: self.secondary_cache
        }[primary]

    def _get_retry_source(self, primary):
        chaos = {True: self.primary_retry_chaos, False: self.secondary_retry_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            raise ClientError({"Error": {}}, "chaos")
        return {
            True: self.primary_retry_source,
            False: self.secondary_retry_source
        }[primary]

    def get_message(self):
        return self.primary_stream_source.recv() or \
            self.secondary_stream_source.recv() or \
            self.primary_retry_source.recv() or \
            self.secondary_retry_source.recv()

    def send_next_event_for_dispatch(self, context, data, correlation_id, delay=0, primary=True, recovering=False):
        if 'send_next_event_for_dispatch' in self.callbacks:
            self.callbacks['send_next_event_for_dispatch']()
        if recovering:
            self._get_retry_source(primary).send(data)
        else:
            self._get_stream_source(primary).send(data)
        self.all_sources.send(data)
        return {'test': 'stub'}

    def start_retries(self, context, run_at, payload, primary=True, recovering=False):
        if recovering:
            self._get_stream_source(primary).send(payload)
        else:
            self._get_retry_source(primary).send(payload)
        self.all_sources.send(payload)
        return {'test': 'stub'}

    def set_message_dispatched(self, correlation_id, steps, retries, primary=True):
        chaos = {True: self.primary_cache_chaos, False: self.secondary_cache_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            return 0
        else:
            key = '%s-%s' % (correlation_id, steps)
            self._get_cache_source(primary)[key] = True
            self.all_caches[key] = True
            return True

    def get_message_dispatched(self, correlation_id, steps, primary=True):
        chaos = {True: self.primary_cache_chaos, False: self.secondary_cache_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            return 0
        else:
            return self._get_cache_source(primary).get('%s-%s' % (correlation_id, steps))

    def acquire_lease(self, correlation_id, steps, retries, primary=True, timeout=None):
        chaos = {True: self.primary_cache_chaos, False: self.secondary_cache_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            return 0
        else:
            key = 'lease-%s-%s' % (correlation_id, steps)
            if key in self._get_cache_source(primary):
                return False
            else:
                self._get_cache_source(primary)[key] = True
                self.all_caches[key] = True
                return True

    def release_lease(self, correlation_id, steps, retries, fence_token, primary=True):
        chaos = {True: self.primary_cache_chaos, False: self.secondary_cache_chaos}[primary]
        if chaos and random.uniform(0.0, 1.0) < chaos:
            return 0
        else:
            key = 'lease-%s-%s' % (correlation_id, steps)
            if key in self._get_cache_source(primary):
                self._get_cache_source(primary).pop(key)
                return True
            else:
                return False

    def increment_error_counters(self, data, dimensions):
        self.errors.send(json.dumps((data, dimensions), **json_dumps_additional_kwargs()))
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
        self.all_caches.clear()
        self.errors.reset()
        self.primary_stream_chaos = 0.0
        self.secondary_stream_chaos = 0.0
        self.primary_retry_chaos = 0.0
        self.secondary_retry_chaos = 0.0
        self.primary_cache_chaos = 0.0
        self.secondary_cache_chaos = 0.0
        self.empty_primary_cache = False
        self.empty_secondary_cache = False
        self.callbacks = {}


def to_kinesis_message(data):
    return {
        "eventSource": "aws:kinesis",
        "kinesis": {
            "data": base64.b64encode(data.encode('utf-8'))
        }
    }


def to_sqs_message(data):
    return {
        "eventSource": "aws:sqs",
        "body": data
    }


def to_sns_message(data):
    return {
        "eventSource": "aws:sns",
        "Sns": {
            "Message": data
        }
    }


class BaseFunctionalTest(unittest.TestCase):

    MESSAGE_TYPE = AWS_CONSTANTS.KINESIS

    def _execute(self, aws, machine_name, context,
                 primary_stream_chaos=0.0,
                 secondary_stream_chaos=0.0,
                 primary_retry_chaos=0.0,
                 secondary_retry_chaos=0.0,
                 primary_cache_chaos=0.0,
                 secondary_cache_chaos=0.0,
                 empty_primary_cache=False,
                 empty_secondary_cache=False):
        start_state_machine(machine_name, context, correlation_id='correlation_id')
        if primary_stream_chaos:
            aws.primary_stream_chaos = primary_stream_chaos
        if secondary_stream_chaos:
            aws.secondary_stream_chaos = secondary_stream_chaos
        if primary_retry_chaos:
            aws.primary_retry_chaos = primary_retry_chaos
        if secondary_retry_chaos:
            aws.secondary_retry_chaos = secondary_retry_chaos
        if primary_cache_chaos:
            aws.primary_cache_chaos = primary_cache_chaos
        if secondary_cache_chaos:
            aws.secondary_cache_chaos = secondary_cache_chaos
        if empty_primary_cache:
            aws.empty_primary_cache = empty_primary_cache
        if empty_secondary_cache:
            aws.empty_secondary_cache = empty_secondary_cache
        message = aws.get_message()
        while message:
            if AWS_CONSTANTS.KINESIS == self.MESSAGE_TYPE:
                handler.lambda_kinesis_handler(to_kinesis_message(message), {})
            elif AWS_CONSTANTS.SQS == self.MESSAGE_TYPE:
                handler.lambda_sqs_handler(to_sqs_message(message), {})
            elif AWS_CONSTANTS.SNS == self.MESSAGE_TYPE:
                handler.lambda_sns_handler(to_sns_message(message), {})
            message = aws.get_message()
