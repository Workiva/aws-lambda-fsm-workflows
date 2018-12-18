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
import logging
import random
import time

# library imports

# application imports
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.action import Action
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string


class StartSagaAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'


class CarAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'


class HotelAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'


class PlaneAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'


class EndSagaAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'


class FailAction(Action):
    """
    Descriptive comment.
    """

    def execute(self, context, obj):
        return 'done'
