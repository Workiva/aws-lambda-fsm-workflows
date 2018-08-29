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
import base64
import os
import json
import logging

# application imports
from aws_lambda_fsm.action import Action
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import store_environment
from aws_lambda_fsm.aws import get_primary_stream_source
from aws_lambda_fsm.aws import get_secondary_stream_source
from aws_lambda_fsm.fsm import Context
from aws_lambda_fsm.constants import ENVIRONMENT
from aws_lambda_fsm.constants import AWS_ECS

logger = logging.getLogger(__name__)

TASK_DETAILS_KEY = 'task_details'
CLUSTER_ARN_KEY = 'cluster_arn'
RUNNER_TASK_DEFINITION_KEY = 'runner_task_definition'
RUNNER_CONTAINER_NAME_KEY = 'runner_container_name'
CONTAINER_IMAGE_KEY = 'container_image'
ENVIRONMENT_KEY = 'environment'
CLONE_AWS_CREDENTIALS_KEY = 'clone_aws_credentials'

DEFAULT_RUNNER_TASK_NAME = 'aws-lambda-fsm'
DEFAULT_RUNNER_CONTAINER_NAME = 'aws-lambda-fsm'


def _testing(environment):
    if 'AWS_HOSTNAME' in os.environ:
        environment.append({'name': 'AWS_HOSTNAME', 'value': os.environ['AWS_HOSTNAME']})
    environment.append({'name': 'AWS_DEFAULT_REGION', 'value': os.environ['AWS_DEFAULT_REGION']})
    if os.environ.get('SQS_URI'):
        environment.append({'name': 'SQS_URI', 'value': os.environ['SQS_URI']})
    if os.environ.get('KINESIS_URI'):
        environment.append({'name': 'KINESIS_URI', 'value': os.environ['KINESIS_URI']})
    if os.environ.get('DYNAMODB_URI'):
        environment.append({'name': 'DYNAMODB_URI', 'value': os.environ['DYNAMODB_URI']})
    for line in open(os.path.expanduser('~/.aws/credentials'), 'r').readlines():
        if " = " in line:
            environment.append({'name': line.split(' = ')[0].upper(), 'value': line.split(' = ')[1].strip()})


class ECSTaskEntryAction(Action):
    """
    Starts a docker task in AWS EC2 Container Service
    """

    def execute(self, context, obj):
        """
        Action that launches and ECS task.

        The API for using this class is as follows:

        {
           'context_var': 'context_value',              # normal context variable
           'task_details': {                            # dictionary of all the states that run images
              'state_name_1': {                         # first state name (as in fsm.yaml)
                                                        # cluster to run image for state_name_1
                'cluster_arn': 'arn:aws:ecs:region:1234567890:cluster/foobar',
                'container_image': 'host/corp/image:12345' # image for state_name_1
              },
              'state_name_2': {                         # second state name (as in fsm.yaml)
                'cluster_arn': 'arn:aws:ecs:eu-west-1:1234567890:cluster/foobar',
                'container_image': 'host/corp/image:12345',
                'runner_task_definition': 'my_runner',  # alternative docker image runner task name
                'runner_container_name': 'my_runner'    # alternative docker image runner container name
              }
            },
            'clone_aws_credentials': True               # flag to copy aws creds from local environment
                                                        # to the container overrides - makes for easier
                                                        # local testing. alternatively, just add permanent
                                                        # credentials to your runner task.
        }

        :param context: a aws_lambda_fsm.fsm.Context instance
        :param obj: a dict
        :return: a string event, or None
        """

        # construct a version of the context that can be base64 encoded
        # and stuffed into a environment variable for the container program.
        # all the container program needs to do is extract this data, add
        # an event, and send the message onto sqs/kinesis/... since this is an
        # ENTRY action, we inspect the current transition for the state we
        # will be in AFTER this code executes.
        ctx = Context.from_payload_dict(context.to_payload_dict())
        ctx.current_state = context.current_transition.target
        ctx.steps += 1
        fsm_context = base64.b64encode(json.dumps(ctx.to_payload_dict()))

        # now finally launch the ECS task using all the data from above
        # as well as tasks etc. specified when the state machine was run.
        state_to_task_details_map = context[TASK_DETAILS_KEY]
        task_details = state_to_task_details_map[context.current_transition.target.name]

        # this is the image the user wants to run
        cluster_arn = task_details[CLUSTER_ARN_KEY]
        container_image = task_details[CONTAINER_IMAGE_KEY]

        # this is the task that will run that image
        task_definition = task_details.get(RUNNER_TASK_DEFINITION_KEY, DEFAULT_RUNNER_TASK_NAME)
        container_name = task_details.get(RUNNER_CONTAINER_NAME_KEY, DEFAULT_RUNNER_CONTAINER_NAME)

        # setup the environment for the ECS task. this first set of variables
        # are used by the docker container runner image.
        environment = {
            ENVIRONMENT.FSM_CONTEXT: fsm_context,
            ENVIRONMENT.FSM_DOCKER_IMAGE: container_image
        }
        # this second set of variables are used by actual docker image that
        # does actual stuff (pdf processing etc.)
        for name, value in task_details.get(ENVIRONMENT_KEY, {}).items():
            environment[name] = value

        # store the environment and record the guid.
        guid, _ = store_environment(context, environment)

        # stuff the guid and a couple stream settings into the task
        # overrides. the guid allows the FSM_CONTEXT to be loaded from
        # storage, and the FSM_PRIMARY_STREAM_SOURCE allow the call
        # to send_next_event_for_dispatch call to succeed.
        env = [
            {
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.NAME: ENVIRONMENT.FSM_ENVIRONMENT_GUID_KEY,
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.VALUE: guid
            },
            {
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.NAME: ENVIRONMENT.FSM_PRIMARY_STREAM_SOURCE,
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.VALUE: get_primary_stream_source() or ''
            },
            {
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.NAME: ENVIRONMENT.FSM_SECONDARY_STREAM_SOURCE,
                AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.VALUE: get_secondary_stream_source() or ''
            }
        ]

        # this is for local testing
        if context.get(CLONE_AWS_CREDENTIALS_KEY):
            _testing(env)

        # get an ECS connection and start a task.
        conn = get_connection(cluster_arn)

        # run the task
        conn.run_task(
            cluster=cluster_arn,
            taskDefinition=task_definition,
            overrides={
                AWS_ECS.CONTAINER_OVERRIDES.KEY: [
                    {
                        AWS_ECS.CONTAINER_OVERRIDES.CONTAINER_NAME: container_name,
                        AWS_ECS.CONTAINER_OVERRIDES.ENVIRONMENT.KEY: env
                    }
                ]
            }
        )

        # entry actions do not return events
        return None
