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
            '{"system_context": {"correlation_id": "correlation_id", "current_event": '
            '"pseudo_init", "current_state": "pseudo_init", "machine_name": "name", "retries": 0, '
            '"started_at": 12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
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
            ['{"system_context": {"correlation_id": "a", "current_event": "pseudo_init", '
             '"current_state": "pseudo_init", "machine_name": "name", "retries": 0, "started_at": '
             '12345, "steps": 0}, "user_context": {"aaa": "bbb"}, "version": "0.1"}',
             '{"system_context": {"correlation_id": "b", "current_event": "pseudo_init", '
             '"current_state": "pseudo_init", "machine_name": "name", "retries": 0, "started_at": '
             '12345, "steps": 0}, "user_context": {"ccc": "ddd"}, "version": "0.1"}'],
            ['a', 'b']
        )
