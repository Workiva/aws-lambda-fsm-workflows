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

import time
from aws_lambda_fsm.action import Action


class LongPause(Action):
    def execute(self, context, obj):
        time.sleep(5)
        return 'ok'


class ReturnOK(Action):
    def execute(self, context, obj):
        if [context.steps, context.retries] in context.get('fail_at', []):
            raise Exception()
        return 'ok'


COUNTER = 0


def get_counter():
    global COUNTER
    return COUNTER


def set_counter(counter):
    global COUNTER
    COUNTER = counter


class IncrementCounter(Action):

    def execute(self, context, obj):
        global COUNTER
        if [context.steps, context.retries] in context.get('fail_at', []):
            raise Exception()
        COUNTER += 1
        context['counter'] = context.get('counter', 0) + 1
        return 'ok' if (context['counter'] < context['loops']) else 'done'


class ResetCounter(Action):

    def execute(self, context, obj):
        del context['counter']
        return 'done'


class SerializationProblem(Action):
    def execute(self, context, obj):
        context['error'] = Exception()
        return 'ok'
