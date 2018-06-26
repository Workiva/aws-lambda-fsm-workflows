# Copyright 2016-2018 Workiva Inc.
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
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class Action(object):
    """
    State machine action.
    """

    def __init__(self, name, event=None):
        """
        Construct a state machine action.

        :param name: a str name for the action.
        :param event: an optional default str event to return.
        """
        self.name = name
        self.event = event

    def execute(self, context, obj):
        """
        Execute the state machine action. This class is typically sub-classed
        to provide useful functionality.

        :param context: an aws_lambda_fsm.fsm.Context instance.
        :param obj: a dict.
        :return: a str event.
        """
        logger.info('action.name=%s', self.name)
        return self.event


def max_retry_event(event):
    """
    A decorator for `Action.execute` which catches an exception on the last
    retry and instead returns the given event.

    :param event: a str event.
    :return: a decorator.

    Example:
        class MyAction(Action):
            @max_retry_event('fail')
            def execute(self, context, obj):
                ...
    """
    def _max_retry_event(func):
        @wraps(func)
        def wrapper(self, context, obj):
            try:
                return func(self, context, obj)
            except:
                if context.retries >= context.max_retries:
                    return event

                raise

        return wrapper

    return _max_retry_event
