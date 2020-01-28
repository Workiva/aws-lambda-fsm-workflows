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
import mock

# application imports
from aws_lambda_fsm.transition import Transition
from aws_lambda_fsm.fsm import Context


class TestTransition(unittest.TestCase):

    def test_execute_no_action(self):
        context = Context('name')
        self.assertEqual(None, context.current_state)
        t = Transition('name', 'target')
        t.execute(context, None)
        self.assertEqual('target', context.current_state)

    def test_execute_with_action(self):
        context = Context('name', initial_user_context={'foo': 'bar'})
        self.assertEqual(None, context.current_state)
        action = mock.Mock()
        t = Transition('name', 'target', action=action)
        t.execute(context, 'obj')
        self.assertEqual('target', context.current_state)
        action.execute.assert_called_with({'foo': 'bar'}, 'obj')
