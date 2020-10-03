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


class Transition(object):
    """
    State machine transition.
    """

    def __init__(self, name, target, action=None, local=False):
        """
        Construct a state machine transition.

        :param name: a str name for the transition.
        :param target: an aws_lambda_fsm.state.State instance corresponding to the destination state of this transition.
        :param action: an optional aws_lambda_fsm.action.Action instance.
        :param local: True if the transition can be executed locally.
        """
        self.name = name
        self.target = target
        self.action = action
        self.local = local

    def execute(self, context, obj):
        """
        Execute the state machine transition.

        :param context: an aws_lambda_fsm.fsm.Context instance.
        :param obj: a dict.
        """
        if self.action:
            self.action.execute(context, obj)
        context.current_state = self.target
