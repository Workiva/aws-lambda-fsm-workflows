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
import base64
import json

# library imports
import mock

# application imports
from aws_lambda_fsm.handler import _get_event_source
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
from aws_lambda_fsm.serialization import json_dumps_additional_kwargs


class TestHandler(unittest.TestCase):

    def test_get_event_source(self):
        self.assertEqual('foo', _get_event_source({'eventSource': 'foo'}))

    def test_get_event_source_caps(self):
        self.assertEqual('foo', _get_event_source({'EventSource': 'foo'}))

    def test_get_event_source_missing(self):
        self.assertEqual(None, _get_event_source({'foo': 'bar'}))

    @mock.patch('aws_lambda_fsm.fsm.FSM')
    def test_process_payload(self,
                             mock_FSM):
        payload = json.dumps({'system_context': {'machine_name': 'barfoo',
                                                 'current_state': 'foobar',
                                                 'stream': 's',
                                                 'table': 't',
                                                 'topic': 'z',
                                                 'metrics': 'm'},
                              'user_context': {}},
                             **json_dumps_additional_kwargs())
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
                              'user_context': {}},
                             **json_dumps_additional_kwargs())
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
        context = {
            'gateway': 'context'
        }
        lambda_api_handler(event, context)
        mock_process_payload.assert_called_with(
            '{"foo": "bar"}', {'source': 'gateway', 'lambda_context': {'gateway': 'context'}})

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_api_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        event = {
            'foo': 'bar'
        }
        context = {
            'gateway': 'context'
        }
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_api_handler(event, context)
        mock_logging.exception.assert_called_with(
            'Critical error handling lambda: %s', '[REDACTED]'
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
        context = {
            'step': 'context'
        }
        lambda_step_handler(event, context)
        mock_process_payload_step.assert_called_with(
            '{"foo": "bar"}', {'source': 'step_function', 'lambda_context': {'step': 'context'}})

################################################################################
# START: kinesis tests
################################################################################

    def get_kinesis_record(self):
        return {
            'eventSource': 'aws:kinesis',
            'kinesis': {
                'data': base64.b64encode(
                    json.dumps({'machine_name': 'barfoo'}, **json_dumps_additional_kwargs()).encode('utf-8'))
            }
        }

    def get_kinesis_context(self):
        return {
            'kinesis': 'context'
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_kinesis_handler(self,
                                    mock_process_payload):
        lambda_kinesis_handler(self.get_kinesis_record(), self.get_kinesis_context())
        mock_process_payload.assert_called_with(
            b'{"machine_name": "barfoo"}',
            {'source': 'kinesis',
             'lambda_record': self.get_kinesis_record(),
             'lambda_context': self.get_kinesis_context()}
        )

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_kinesis_handler_error(self,
                                          mock_logging,
                                          mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_kinesis_handler(self.get_kinesis_record(), self.get_kinesis_context())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:kinesis', 'kinesis': {'data': '[REDACTED]'}}
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

    def get_dynamodb_context(self):
        return {
            'dynamodb': 'context'
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_dynamodb_handler(self,
                                     mock_process_payload):
        lambda_dynamodb_handler(self.get_dynamodb_record(), self.get_dynamodb_context())
        mock_process_payload.assert_called_with(
            '{"pay":"load"}',
            {'source': 'dynamodb_stream',
             'lambda_record': self.get_dynamodb_record(),
             'lambda_context': self.get_dynamodb_context()}
        )

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_dynamodb_handler_error(self,
                                           mock_logging,
                                           mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_dynamodb_handler(self.get_dynamodb_record(), self.get_dynamodb_context())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:dynamodb', 'dynamodb': {'NewImage': {'payload': {'S': '[REDACTED]'}}}}
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
        lambda_timer_handler({}, {})
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
        lambda_timer_handler({}, {})
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
                'Message': json.dumps({"mess": "age"}, **json_dumps_additional_kwargs())
            }
        }

    def get_sns_context(self):
        return {
            'sns': 'context'
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_sns_handler(self,
                                mock_process_payload):
        lambda_sns_handler(self.get_sns_record(), self.get_sns_context())
        mock_process_payload.assert_called_with(
            '{"mess": "age"}',
            {'source': 'sns', 'lambda_record': self.get_sns_record(), 'lambda_context': self.get_sns_context()}
        )

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_sns_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_sns_handler(self.get_sns_record(), self.get_sns_context())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:sns', 'Sns': {'Message': '[REDACTED]'}}
        )

################################################################################
# START: sqs tests
################################################################################

    def get_sqs_record(self):
        return {
            'eventSource': 'aws:sqs',
            'body': json.dumps({"mess": "age"}, **json_dumps_additional_kwargs())
        }

    def get_sqs_context(self):
        return {
            'sqs': 'context'
        }

    @mock.patch('aws_lambda_fsm.handler._process_payload')
    def test_lambda_sqs_handler(self,
                                mock_process_payload):
        lambda_sqs_handler(self.get_sqs_record(), self.get_sqs_context())
        mock_process_payload.assert_called_with(
            '{"mess": "age"}',
            {'source': 'sqs', 'lambda_record': self.get_sqs_record(), 'lambda_context': self.get_sqs_context()}
        )

    @mock.patch('aws_lambda_fsm.handler.FSM')
    @mock.patch('aws_lambda_fsm.handler.logger')
    def test_lambda_sqs_handler_error(self,
                                      mock_logging,
                                      mock_FSM):
        mock_FSM.return_value.create_FSM_instance.side_effect = Exception()
        lambda_sqs_handler(self.get_sqs_record(), self.get_sqs_context())
        mock_logging.exception.assert_called_with(
            'Critical error handling record: %s',
            {'eventSource': 'aws:sqs', 'body': '[REDACTED]'}
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
        mock_lambda_timer_handler.assert_called_with({'source': 'aws.events'}, 'a')
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
        mock_lambda_kinesis_handler.assert_called_with({'eventSource': 'aws:kinesis', 'kinesis': 'abc123'}, 'a')
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
        mock_lambda_dynamodb_handler.assert_called_with({'eventSource': 'aws:dynamodb', 'dynamodb': {}}, 'a')
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
        mock_lambda_sns_handler.assert_called_with({'eventSource': 'aws:sns', 'Sns': {'Message': 'message'}}, 'a')
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
        mock_lambda_sqs_handler.assert_called_with({'eventSource': 'aws:sqs', 'body': '{"Message": "message"}'}, 'a')
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
        mock_lambda_api_handler.assert_called_with({'foo': 'bar'}, 'a')
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
        mock_lambda_step_handler.assert_called_with({'step_function': True, 'foo': 'bar'}, 'a')
