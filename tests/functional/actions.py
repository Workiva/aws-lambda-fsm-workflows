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

from aws_lambda_fsm.action import Action


class ReturnOK(Action):
    def execute(self, context, obj):
        if [context.steps, context.retries] in context.get('fail_at', []):
            raise Exception()
        return 'ok'


class IncrementCounter(Action):
    def execute(self, context, obj):
        if [context.steps, context.retries] in context.get('fail_at', []):
            raise Exception()
        context['counter'] = context.get('counter', 0) + 1
        return 'ok' if (context['counter'] < context['loops']) else 'done'
