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

# library imports
import mock

# application imports
from aws_lambda_fsm.client import start_state_machine
from aws_lambda_fsm.client import start_state_machines


class TestClient(unittest.TestCase):

    @mock.patch('aws_lambda_fsm.client.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.client.time')
    def test_start_state_machine(self,
                                 mock_time,
                                 mock_send_next_event_for_dispatch):
        mock_time.time.return_value = 12345.
        start_state_machine('name', {'aaa': 'bbb'}, correlation_id='correlation_id')
        mock_send_next_event_for_dispatch.assert_called_with(
            None,
            '{"system_context": {"additional_delay_seconds": 0, "correlation_id": "correlation_id", '
            '"current_event": "pseudo_init", "current_state": "pseudo_init", "machine_name": "name", '
            '"retries": 0, "started_at": 12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
            'correlation_id'
        )

    @mock.patch('aws_lambda_fsm.client.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.client.time')
    def test_start_state_machine_additional_delay_seconds(self,
                                                          mock_time,
                                                          mock_send_next_event_for_dispatch):
        mock_time.time.return_value = 12345.
        start_state_machine('name', {'aaa': 'bbb'}, correlation_id='correlation_id', additional_delay_seconds=999)
        mock_send_next_event_for_dispatch.assert_called_with(
            None,
            '{"system_context": {"additional_delay_seconds": 999, "correlation_id": "correlation_id", '
            '"current_event": "pseudo_init", "current_state": "pseudo_init", "machine_name": "name", '
            '"retries": 0, "started_at": 12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
            'correlation_id'
        )

    @mock.patch('aws_lambda_fsm.client.send_next_events_for_dispatch')
    @mock.patch('aws_lambda_fsm.client.time')
    def test_start_state_machines(self,
                                  mock_time,
                                  mock_send_next_event_for_dispatch):
        mock_time.time.return_value = 12345.
        start_state_machines('name', [{'aaa': 'bbb'}, {'ccc': 'ddd'}], correlation_ids=['a', 'b'])
        mock_send_next_event_for_dispatch.assert_called_with(
            None,
            ['{"system_context": {"additional_delay_seconds": 0, "correlation_id": "a", "current_event": '
             '"pseudo_init", "current_state": "pseudo_init", "machine_name": "name", "retries": 0, '
             '"started_at": 12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
             '{"system_context": {"additional_delay_seconds": 0, "correlation_id": "b", "current_event": '
             '"pseudo_init", "current_state": "pseudo_init", "machine_name": "name", "retries": 0, '
             '"started_at": 12345, "steps": 0}, "user_context": {"ccc": "ddd"}, "version": "0.1"}'],
            ['a', 'b']
        )

    @mock.patch('aws_lambda_fsm.client.send_next_events_for_dispatch')
    @mock.patch('aws_lambda_fsm.client.time')
    def test_start_state_machines_additional_delay_seconds(self,
                                                           mock_time,
                                                           mock_send_next_event_for_dispatch):
        mock_time.time.return_value = 12345.
        start_state_machines('name', [{'aaa': 'bbb'}, {'ccc': 'ddd'}], correlation_ids=['a', 'b'],
                             additional_delay_seconds=999)
        mock_send_next_event_for_dispatch.assert_called_with(
            None,
            ['{"system_context": {"additional_delay_seconds": 999, "correlation_id": "a", "current_event": '
             '"pseudo_init", "current_state": "pseudo_init", "machine_name": "name", "retries": 0, '
             '"started_at": 12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
             '{"system_context": {"additional_delay_seconds": 999, "correlation_id": "b", "current_event": '
             '"pseudo_init", "current_state": "pseudo_init", "machine_name": "name", "retries": 0, '
             '"started_at": 12345, "steps": 0}, "user_context": {"ccc": "ddd"}, "version": "0.1"}'],
            ['a', 'b']
        )
