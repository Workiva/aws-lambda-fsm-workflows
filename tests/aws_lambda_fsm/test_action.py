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

# library imports

# application imports
from aws_lambda_fsm.action import Action, max_retry_event
from aws_lambda_fsm.fsm import Context


class TestAction(unittest.TestCase):

    def test_execute(self):
        action = Action('name', event='foo')
        event = action.execute('', 'event')
        self.assertEqual('foo', event)


class TestMaxRetryEvent(unittest.TestCase):
    class MyAction(Action):
        @max_retry_event('fail')
        def execute(self, context, obj):
            raise RuntimeError('Test Exception.')

    def test_more_retries(self):
        context = Context('name', max_retries=1)
        context.retries = 0

        action = self.MyAction('name')

        with self.assertRaises(RuntimeError):
            action.execute(context, None)

    def test_max_retry(self):
        context = Context('name', max_retries=1)
        context.retries = 1

        action = self.MyAction('name')

        event = action.execute(context, None)

        self.assertEqual(event, 'fail')
