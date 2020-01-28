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
from aws_lambda_fsm.state import State
from aws_lambda_fsm.fsm import Context


class TestAction(unittest.TestCase):

    def test_add_transition(self):
        state = State('name')
        state.add_transition('transition', 'event')
        self.assertEqual({'event': 'transition'}, state._event_2_transition)

    def test_get_transition_missing(self):
        state = State('name')
        self.assertRaises(KeyError, state.get_transition, 'event')

    def test_get_transition(self):
        state = State('name')
        state.add_transition('transition', 'event')
        self.assertEqual('transition', state.get_transition('event'))

    def test_dispatch(self):
        entry_action = mock.Mock()
        do_action = mock.Mock()
        exit_action = mock.Mock()
        transition = mock.Mock()
        state = State('name',
                      entry_action=entry_action,
                      do_action=do_action,
                      exit_action=exit_action)
        state.add_transition(transition, 'event')
        context = Context('name',
                          initial_state=state,
                          initial_system_context={'sys': 'tem'},
                          initial_user_context={'foo': 'bar'})
        event = state.dispatch(context, 'event', 'obj')
        exit_action.execute.assert_called_with({'foo': 'bar'}, 'obj')
        do_action.execute.assert_called_with({'foo': 'bar'}, 'obj')
        entry_action.execute.assert_called_with({'foo': 'bar'}, 'obj')
        transition.execute.assert_called_with({'foo': 'bar'}, 'obj')
        self.assertIsNotNone(event)

    def test_dispatch_to_final_no_event(self):
        entry_action = mock.Mock()
        do_action = mock.Mock()
        exit_action = mock.Mock()
        transition = mock.Mock()
        state = State('name',
                      entry_action=entry_action,
                      do_action=do_action,
                      exit_action=exit_action,
                      final=True)
        state.add_transition(transition, 'event')
        context = Context('name', initial_state=state)
        context['foo'] = 'bar'
        event = state.dispatch(context, 'event', 'obj')
        self.assertIsNone(event)
