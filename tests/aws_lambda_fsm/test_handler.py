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
import unittest
import base64
import json

# library imports
import mock

# application imports
from aws_lambda_fsm.handler import _process_payload
from aws_lambda_fsm.handler import _process_payload_step
from aws_lambda_fsm.handler import lambda_dynamodb_handler
from aws_lambda_fsm.handler import lambda_kinesis_handler
from aws_lambda_fsm.handler import lambda_timer_handler
from aws_lambda_fsm.handler import lambda_sns_handler
from aws_lambda_fsm.handler import lambda_sqs_handler
from aws_lambda_fsm.handler import lambda_handler
from aws_lambda_fsm.handler import lambda_api_handler
from aws_lambda_fsm.handler import lambda_step_handler


class TestHandler(unittest.TestCase):

    @mock.patch('aws_lambda_fsm.fsm.FSM')
    def test_process_payload(self,
                             mock_FSM):
        payload = json.dumps({'system_context': {'machine_name': 'barfoo',
                                                 'current_state': 'foobar',
                                                 'stream': 's',
                                                 'table': 't',
                                                 'topic': 'z',
                                                 'metrics': 'm'},
                              'user_context': {}}, sort_keys=True)
        obj = {}
        mock_FSM.return_value.create_FSM_instance.return_value\
            .system_context.return_value.get.return_value = 'pseudo-init'
        _process_payload(payload, obj)
        mock_FSM.return_value.create_FSM_instance.assert_called_with(
            'barfoo',
            initial_system_context={'topic': 'z',
                                    'machine_name': 'barfoo',
                                    'stream': 's',
                                    'current_state': 'foobar',
                                    'metrics': 'm',
                                    'table': 't'},
            initial_user_context={},
            initial_state_name='foobar'
        )
        mock_FSM.return_value.create_FSM_instance.return_value.dispatch.assert_called_with(
            'pseudo-init',
            {'payload': '{"system_context": {"current_state": "foobar", "machine_name": '
                        '"barfoo", "metrics": "m", "stream": "s", "table": "t", '
                        '"topic": "z"}, "user_context": {}}'}
        )
        self.assertEqual({'payload': payload}, obj)

    @mock.patch('aws_lambda_fsm.fsm.FSM')
    def test_process_payload_step(self,
                                  mock_FSM):
        payload = json.dumps({'system_context': {'machine_name': 'barfoo',
                                                 'current_state': 'foobar',
                                                 'stream': 's',
                                                 'table': 't',
                                                 'topic': 'z',
                                                 'metrics': 'm'},
                              'user_context': {}}, sort_keys=True)
        obj = {}
        mock_FSM.return_value.create_FSM_instance.return_value\
            .system_context.return_value.get.return_value = 'pseudo-init'
        _process_payload_step(payload, obj)
        mock_FSM.return_value.create_FSM_instance.assert_called_with(
            'barfoo',
            initial_system_context={'topic': 'z',
                                    'machine_name': 'barfoo',
                                    'stream': 's',
                                    'current_state': 'foobar',
                                    'metrics': 'm',
                                    'table': 't'},
            initial_user_context={},
            initial_state_name='foobar'
        )
        mock_FSM.return_value.create_FSM_instance.return_value.current_state.dispatch.assert_called_with(
            mock_FSM.return_value.create_FSM_instance.return_value,
            'pseudo-init',
            {'payload': '{"system_context": {"current_state": "foobar", "machine_name": '
                        '"barfoo", "metrics": "m", "stream": "s", "table": "t", '
                        '"topic": "z"}, "user_context": {}}'}
        )
        self.assertEqual({'payload': payload}, obj)

################################################################################
# START: gateway tests
################################################################################

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_api_gateway_handler(self,
                                 mock_process_payload):
        event = {
            'foo': 'bar'
        }
        lambda_api_handler(event)
        mock_process_payload.assert_called_with('{"foo": "bar"}', {'source': 'gateway'})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_api_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        event = {
            'foo': 'bar'
        }
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_api_handler(event)
        mock_logging.exception.assert_called_with(
            'Critical error handling lambda: %s', {'foo': 'bar'}
        )

################################################################################
# START: step function tests
################################################################################

    @mock.patch('aws_lambda_fsm.handler._process_payload_step')
    def test_step_function_handler(self,
                                   mock_process_payload_step):
        event = {
            'foo': 'bar'
        }
        lambda_step_handler(event)
        mock_process_payload_step.assert_called_with('{"foo": "bar"}', {'source': 'step_function'})

################################################################################
# START: kinesis tests
################################################################################

    def get_kinesis_record(self):
        return {
            'eventSource': 'aws:kinesis',
            'kinesis': {
                'data': base64.b64encode(json.dumps({'machine_name': 'barfoo'}, sort_keys=True))
            }
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_kinesis_handler(self,
                                    mock_process_payload):
        lambda_kinesis_handler(self.get_kinesis_record())
        mock_process_payload.assert_called_with('{"machine_name": "barfoo"}', {'source': 'kinesis'})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_kinesis_handler_error(self,
                                          mock_logging,
                                          mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_kinesis_handler(self.get_kinesis_record())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:kinesis', 'kinesis': {'data': 'eyJtYWNoaW5lX25hbWUiOiAiYmFyZm9vIn0='}}
        )

################################################################################
# START: dynamodb tests
################################################################################

    def get_dynamodb_record(self):
        return {
            'eventSource': 'aws:dynamodb',
            'dynamodb': {
                'NewImage': {
                    'payload': {
                        'S': '{"pay":"load"}'
                    }
                }
            }
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_dynamodb_handler(self,
                                     mock_process_payload):
        lambda_dynamodb_handler(self.get_dynamodb_record())
        mock_process_payload.assert_called_with('{"pay":"load"}', {'source': 'dynamodb_stream'})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_dynamodb_handler_error(self,
                                           mock_logging,
                                           mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_dynamodb_handler(self.get_dynamodb_record())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:dynamodb', 'dynamodb': {'NewImage': {'payload': {'S': '{"pay":"load"}'}}}}
        )

################################################################################
# START: timer tests
################################################################################

    @mock.patch('aws_lambda_fsm.handler.retriable_entities')
    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_timer_handler(self,
                                  mock_process_payload,
                                  mock_retriable_entities):
        mock_retriable_entities.return_value = [{'payload': 'payloadZ', 'correlation_id': 'abc123'}]
        lambda_timer_handler()
        mock_process_payload.assert_called_with('payloadZ', {'source': 'dynamodb_retry'})

    @mock.patch('aws_lambda_fsm.handler.retriable_entities')
    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_timer_handler_error(self,
                                        mock_logging,
                                        mock_FSM,
                                        mock_retriable_entities):
        mock_retriable_entities.return_value = [{'payload': 'payloadZ'}]
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_timer_handler()
        mock_logging.exception.assert_called_with(
            'Critical error handling entity: %s', {'payload': 'payloadZ'}
        )

################################################################################
# START: sns tests
################################################################################

    def get_sns_record(self):
        return {
            'eventSource': 'aws:sns',
            'Sns': {
                'Message': json.dumps({"mess": "age"})
            }
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_sns_handler(self,
                                mock_process_payload):
        lambda_sns_handler(self.get_sns_record())
        mock_process_payload.assert_called_with('{"mess": "age"}', {'source': 'sns'})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_sns_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_sns_handler(self.get_sns_record())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:sns', 'Sns': {'Message': '{"mess": "age"}'}}
        )

################################################################################
# START: sqs tests
################################################################################

    def get_sqs_record(self):
        return {
            'eventSource': 'aws:sqs',
            'body': json.dumps({"mess": "age"})
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_sqs_handler(self,
                                mock_process_payload):
        lambda_sqs_handler(self.get_sqs_record())
        mock_process_payload.assert_called_with('{"mess": "age"}', {'source': 'sqs'})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_sqs_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_sqs_handler(self.get_sqs_record())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:sqs', 'body': '{"mess": "age"}'}
        )


################################################################################
# START: general tests
################################################################################

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_timer(self,
                                  mock_lambda_step_handler,
                                  mock_lambda_api_handler,
                                  mock_lambda_sns_handler,
                                  mock_lambda_sqs_handler,
                                  mock_lambda_timer_handler,
                                  mock_lambda_kinesis_handler,
                                  mock_lambda_dynamodb_handler):
        lambda_handler({'source': 'aws.events'}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        mock_lambda_timer_handler.assert_called_with()
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        self.assertFalse(mock_lambda_sns_handler.called)
        self.assertFalse(mock_lambda_sqs_handler.called)
        self.assertFalse(mock_lambda_api_handler.called)
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_kinesis(self,
                                    mock_lambda_step_handler,
                                    mock_lambda_api_handler,
                                    mock_lambda_sns_handler,
                                    mock_lambda_sqs_handler,
                                    mock_lambda_timer_handler,
                                    mock_lambda_kinesis_handler,
                                    mock_lambda_dynamodb_handler):
        lambda_handler({'Records': [{'eventSource': 'aws:kinesis', 'kinesis': 'abc123'}]}, 'a')
        mock_lambda_kinesis_handler.assert_called_with({'eventSource': 'aws:kinesis', 'kinesis': 'abc123'})
        self.assertFalse(mock_lambda_timer_handler.called)
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        self.assertFalse(mock_lambda_sns_handler.called)
        self.assertFalse(mock_lambda_sqs_handler.called)
        self.assertFalse(mock_lambda_api_handler.called)
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_dynamodb(self,
                                     mock_lambda_step_handler,
                                     mock_lambda_api_handler,
                                     mock_lambda_sns_handler,
                                     mock_lambda_sqs_handler,
                                     mock_lambda_timer_handler,
                                     mock_lambda_kinesis_handler,
                                     mock_lambda_dynamodb_handler):
        lambda_handler({'Records': [{'eventSource': 'aws:dynamodb', 'dynamodb': {}}]}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        self.assertFalse(mock_lambda_timer_handler.called)
        mock_lambda_dynamodb_handler.assert_called_with({'eventSource': 'aws:dynamodb', 'dynamodb': {}})
        self.assertFalse(mock_lambda_sns_handler.called)
        self.assertFalse(mock_lambda_sqs_handler.called)
        self.assertFalse(mock_lambda_api_handler.called)
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_sns(self,
                                mock_lambda_step_handler,
                                mock_lambda_api_handler,
                                mock_lambda_sns_handler,
                                mock_lambda_sqs_handler,
                                mock_lambda_timer_handler,
                                mock_lambda_kinesis_handler,
                                mock_lambda_dynamodb_handler):
        lambda_handler({'Records': [{'eventSource': 'aws:sns', 'Sns': {'Message': 'message'}}]}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        self.assertFalse(mock_lambda_timer_handler.called)
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        mock_lambda_sns_handler.assert_called_with({'eventSource': 'aws:sns', 'Sns': {'Message': 'message'}})
        self.assertFalse(mock_lambda_sqs_handler.called)
        self.assertFalse(mock_lambda_api_handler.called)
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_sqs(self,
                                mock_lambda_step_handler,
                                mock_lambda_api_handler,
                                mock_lambda_sns_handler,
                                mock_lambda_sqs_handler,
                                mock_lambda_timer_handler,
                                mock_lambda_kinesis_handler,
                                mock_lambda_dynamodb_handler):
        lambda_handler({'Records': [{'eventSource': 'aws:sqs', 'body': '{"Message": "message"}'}]}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        self.assertFalse(mock_lambda_timer_handler.called)
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        self.assertFalse(mock_lambda_sns_handler.called)
        mock_lambda_sqs_handler.assert_called_with({'eventSource': 'aws:sqs', 'body': '{"Message": "message"}'})
        self.assertFalse(mock_lambda_api_handler.called)
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_api(self,
                                mock_lambda_step_handler,
                                mock_lambda_api_handler,
                                mock_lambda_sns_handler,
                                mock_lambda_sqs_handler,
                                mock_lambda_timer_handler,
                                mock_lambda_kinesis_handler,
                                mock_lambda_dynamodb_handler):
        lambda_handler({'foo': 'bar'}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        self.assertFalse(mock_lambda_timer_handler.called)
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        self.assertFalse(mock_lambda_sns_handler.called)
        self.assertFalse(mock_lambda_sqs_handler.called)
        mock_lambda_api_handler.assert_called_with({'foo': 'bar'})
        self.assertFalse(mock_lambda_step_handler.called)

    @mock.patch('aws_lambda_fsm.handler.lambda_dynamodb_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_kinesis_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_timer_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sqs_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_sns_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_api_handler')
    @mock.patch('aws_lambda_fsm.handler.lambda_step_handler')
    def test_lambda_handler_step(self,
                                 mock_lambda_step_handler,
                                 mock_lambda_api_handler,
                                 mock_lambda_sns_handler,
                                 mock_lambda_sqs_handler,
                                 mock_lambda_timer_handler,
                                 mock_lambda_kinesis_handler,
                                 mock_lambda_dynamodb_handler):
        lambda_handler({'step_function': True, 'foo': 'bar'}, 'a')
        self.assertFalse(mock_lambda_kinesis_handler.called)
        self.assertFalse(mock_lambda_timer_handler.called)
        self.assertFalse(mock_lambda_dynamodb_handler.called)
        self.assertFalse(mock_lambda_sns_handler.called)
        self.assertFalse(mock_lambda_sqs_handler.called)
        self.assertFalse(mock_lambda_api_handler.called)
        mock_lambda_step_handler.assert_called_with({'step_function': True, 'foo': 'bar'})
