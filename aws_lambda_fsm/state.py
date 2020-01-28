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
import logging

# library imports

# application imports

logger = logging.getLogger(__name__)


class State(object):
    """
    State machine state.
    """

    def __init__(self, name, entry_action=None, do_action=None, exit_action=None,
                 initial=None, final=None):
        """
        Construct a state machine state.

        :param name: a str name for the state.
        :param entry_action: an optional aws_lambda_fsm.action.Action instance to execute on entry to the state.
        :param do_action: an optional aws_lambda_fsm.action.Action instance to execute once in the state.
        :param exit_action: an optional aws_lambda_fsm.action.Action instance to execute on exit from the state.
        """
        self.name = name
        self.entry_action = entry_action
        self.do_action = do_action
        self.exit_action = exit_action
        self.initial = initial
        self.final = final
        self._event_2_transition = {}

    def add_transition(self, transition, event):
        """
        Adds a transition to the state.

        :param transition: an aws_lambda_fsm.transition.Transition instance.
        :param event: a str event which triggers the transition.
        """
        self._event_2_transition[event] = transition

    def get_transition(self, event):
        """
        Returns the transition for the specified event.

        :param event: a str event.
        :return: an aws_lambda_fsm.transition.Transition instance.
        """
        return self._event_2_transition[event]

    def dispatch(self, context, event, obj):
        """
        Dispatch an event to the state machine state.

        :param context: an aws_lambda_fsm.fsm.Context instance
        :param event: a str event.
        :param obj: a dict.
        :return: a str event.
        """
        transition = self.get_transition(event)
        context.current_transition = transition
        if context.current_state.exit_action:
            context.current_action = context.current_state.exit_action
            context.current_state.exit_action.execute(context, obj)
        transition.execute(context, obj)
        if context.current_state.entry_action:
            context.current_action = context.current_state.entry_action
            context.current_state.entry_action.execute(context, obj)
        next_event = None
        if context.current_state.do_action:
            context.current_action = context.current_state.do_action
            next_event = context.current_state.do_action.execute(context, obj)
        if context.current_state.final:
            return None
        return next_event
