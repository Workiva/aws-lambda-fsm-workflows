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
from builtins import str
import logging
import random
import time
import sys

# library imports

# application imports
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.action import Action
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import get_arn_from_arn_string


class IncrementAction(Action):
    """
    Action that simply increments a counter in the context.
    """

    def execute(self, context, obj):
        logging.info('python: %s', sys.version_info)
        logging.info('context: %s', context)

        # randomly raise an exception
        if random.uniform(0, 1.0) < 0.5:
            raise Exception()

        logging.info('action.name=%s', self.name)

        # increment the counter
        context['count'] = context.get('count', 0) + 1

        # set the started_at (user space) variable
        if context['count'] == 1:
            context['started_at'] = int(time.time())

        # when done, emit a dynamodb record
        if context['count'] > 100:
            if 'results_arn' in context:
                table_arn = context['results_arn']
                table_name = get_arn_from_arn_string(table_arn).slash_resource()
                conn = get_connection(table_arn)
                conn.put_item(
                    TableName=table_name,
                    Item={
                        'correlation_id': {AWS_DYNAMODB.STRING: context.correlation_id},
                        'count': {AWS_DYNAMODB.NUMBER: str(context['count'])},
                        'started_at': {AWS_DYNAMODB.NUMBER: str(context['started_at'])},
                        'finished_at': {AWS_DYNAMODB.NUMBER: str(int(time.time()))},
                        'flag': {AWS_DYNAMODB.STRING: context.get('flag', 'Unknown')}
                    }
                )
            return 'done'

        return 'event1'
