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
import copy
import json


# library imports
import mock
from botocore.exceptions import ClientError

# application imports
from aws_lambda_fsm.action import Action
from aws_lambda_fsm.fsm import FSM
from aws_lambda_fsm import config
from aws_lambda_fsm.fsm import Context
from aws_lambda_fsm.fsm import json_dumps_additional_kwargs


class TestAction(Action):
    def execute(self, context, obj):
        return 'c'


class ErrorAction(Action):
    def execute(self, context, obj):
        raise Exception()


class TestFsmBase(unittest.TestCase):
    maxDiff = 99999

    CONFIG_DICT = {
        'machines': [
            {
                'import': 'some/fsm.yaml'
            },
            {
                'name': 'foo',
                'stream': 's',
                'table': 't',
                'metrics': 'm',
                'topic': 'z',
                'states': [
                    {
                        'name': 'a',
                        'do_action': 'tests.aws_lambda_fsm.test_fsm.TestAction',
                        'initial': 'true',
                        'transitions': [
                            {
                                'target': 'b',
                                'event': 'c'
                            }
                        ]
                    },
                    {
                        'name': 'b',
                        'final': 'true'
                    }
                ]
            }
        ]
    }


class TestFSM(TestFsmBase):
    def test_FSM_default(self):
        FSM()

    def test_FSM_empty(self):
        config_dict = {'machines': []}
        fsm = FSM(config_dict=config_dict)
        self.assertEqual({}, fsm.machines)
        fsm = FSM(config_dict=config_dict)
        self.assertEqual({}, fsm.machines)

    def _instance(self, initial_system_context=None):
        # test the factory
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        self.assertEqual({
            'foo': {
                'max_retries': 5,
                'states': {
                    'a': fsm.machines['foo']['states']['a'],
                    'b': fsm.machines['foo']['states']['b'],
                    'pseudo_init': fsm.machines['foo']['states']['pseudo_init'],
                    'pseudo_final': fsm.machines['foo']['states']['pseudo_final']
                },
                'transitions': {
                    'a->b:c': fsm.machines['foo']['transitions']['a->b:c'],
                    'b->pseudo_final:pseudo_final': fsm.machines['foo']['transitions']['b->pseudo_final:pseudo_final'],
                    'pseudo_init->a:pseudo_init': fsm.machines['foo']['transitions']['pseudo_init->a:pseudo_init']

                }
            }
        }, fsm.machines)

        # test the instance
        instance = fsm.create_FSM_instance('foo',
                                           initial_state_name='a',
                                           initial_system_context=initial_system_context)
        return instance

    def test_FSM(self):
        instance = self._instance()
        self.assertEqual({}, instance)

    def test_FSM_system_context(self):
        instance = self._instance()
        instance.system_context()

    def test_FSM_user_context(self):
        instance = self._instance()
        instance['foo'] = 'bar'
        self.assertEqual({'foo': 'bar'}, instance.user_context())

    def test_FSM_properties_set(self):
        instance = self._instance(initial_system_context={'correlation_id': 'foo',
                                                          'steps': 'bar',
                                                          'retries': 'baz'})
        self.assertEqual('foo', instance.correlation_id)
        self.assertEqual('bar', instance.steps)
        self.assertEqual('baz', instance.retries)
        self.assertEqual(None, instance.current_event)
        instance.current_event = 'foo'
        self.assertEqual('foo', instance.current_event)
        self.assertEqual('bar', instance.steps)
        instance.steps = 999
        self.assertEqual(999, instance.steps)

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    def test_FSM_properties_unset(self,
                                  mock_uuid):
        instance = self._instance()
        mock_uuid.uuid4.return_value.hex = 'aaa'
        self.assertEqual('aaa', instance.correlation_id)
        self.assertEqual(0, instance.steps)
        self.assertEqual(0, instance.retries)

    def test_to_payload_dict(self):
        instance = self._instance(initial_system_context={'machine_name': 'foo',
                                                          'current_state': 'a',
                                                          'correlation_id': 'foo',
                                                          'steps': 'bar',
                                                          'retries': 'baz'})
        expected = {'system_context': {'machine_name': 'foo',
                                       'current_state': 'a',
                                       'correlation_id': 'foo',
                                       'retries': 'baz',
                                       'steps': 'bar',
                                       'max_retries': 5},
                    'user_context': {},
                    'version': '0.1'}
        self.assertEqual(expected, instance.to_payload_dict())
        self.assertEqual(
            expected,
            instance.from_payload_dict(instance.to_payload_dict()).to_payload_dict()
        )


class TestDispatchAndRetry(TestFsmBase):
    def _dispatch(self,
                  mock_send_next_event_for_dispatch):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        payload = {
            "system_context": {
                "machine_name": "foo",
                "current_state": "s",
                "current_event": "e",
                "correlation_id": "b",
                "additional_delay_seconds": 0,
                "steps": 999,
                "retries": 0,
                "metrics": "m",
                "stream": "s",
                "topic": "z",
                "table": "t"
            },
            "user_context": {},
            "version": "0.1"
        }
        instance = fsm.create_FSM_instance(
            'foo',
            initial_state_name='pseudo_init',
            initial_system_context=payload['system_context'],
            initial_user_context=payload['user_context']
        )
        mock_send_next_event_for_dispatch.return_value = {'put': 'record'}
        instance._dispatch_and_retry(
            'pseudo_init',
            {
                'payload': json.dumps(payload, **json_dumps_additional_kwargs()),
                'source': 'dynamodb_retry'
            }
        )
        self.assertEqual(instance.retries, 0)
        return instance

    @mock.patch('aws_lambda_fsm.fsm.get_message_dispatched')
    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    def test_dispatch_already_dispatched(self,
                                         mock_stop_retries,
                                         mock_queue_error,
                                         mock_get_message_dispatched):
        mock_get_message_dispatched.return_value = True
        instance = self._dispatch(mock.Mock())
        mock_queue_error.assert_called_with(
            'duplicate',
            'Message has been processed already (True).'
        )
        mock_stop_retries.assert_called_with(
            instance,
            primary=True
        )

    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.increment_error_counters')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    def test_send_errors(self,
                         mock_set_message_dispatched,
                         mock_increment_error_counter,
                         mock_store_checkpoint,
                         mock_send_next_event_for_dispatch,
                         mock_stop_retries):
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        instance._queue_error('a', 'amsg')
        instance._queue_error('b', 'bmsg')
        instance._send_queued_errors()
        mock_increment_error_counter.assert_called_with(
            {'a': 1, 'b': 1},
            dimensions={'current_event': 'e', 'machine_name': 'foo', 'current_state': 's'}
        )

    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.Context._send_queued_errors')
    def test_dispatch_raised_send_errors(self,
                                         mock_send_errors,
                                         mock_store_checkpoint,
                                         mock_send_next_event_for_dispatch,
                                         mock_start_retries):
        mock_store_checkpoint.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        mock_send_errors.side_effect = Exception()
        self._dispatch(mock_send_next_event_for_dispatch)

    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    def test_dispatch(self,
                      mock_set_message_dispatched,
                      mock_store_checkpoint,
                      mock_send_next_event_for_dispatch,
                      mock_stop_retries):
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        mock_send_next_event_for_dispatch.assert_called_with(
            instance,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "c", "current_state": '
            '"a", "machine_name": "foo", "max_retries": 5, "metrics": "m", "retries": 0, "steps": 1000, "stream": '
            '"s", "table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            'b',
            delay=0,
            primary=True,
            recovering=False
        )
        mock_store_checkpoint.assert_called_with(
            instance,
            '{"put": "record"}',
            primary=True
        )
        mock_stop_retries.assert_called_with(
            instance,
            primary=True
        )
        mock_set_message_dispatched.assert_called_with(
            'b',
            999,
            0,
            primary=False
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    def test_dispatch_fails_set_message_dispatched_after_kinesis_put(self,
                                                                     mock_set_message_dispatched,
                                                                     mock_store_checkpoint,
                                                                     mock_send_next_event_for_dispatch,
                                                                     mock_stop_retries,
                                                                     mock_queue_error):
        mock_set_message_dispatched.return_value = False
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        mock_send_next_event_for_dispatch.assert_called_with(
            instance,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "c", "current_state": '
            '"a", "machine_name": "foo", "max_retries": 5, "metrics": "m", "retries": 0, "steps": 1000, "stream": "s", '
            '"table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            'b',
            delay=0,
            primary=True,
            recovering=False
        )
        mock_store_checkpoint.assert_called_with(
            instance,
            '{"put": "record"}',
            primary=True
        )
        mock_stop_retries.assert_called_with(
            instance,
            primary=True
        )

        mock_set_message_dispatched.assert_has_calls(
            [
                mock.call('b', 999, 0, primary=True),
                mock.call('b', 999, 0, primary=False)
            ]
        )
        mock_queue_error.assert_called_with(
            'cache',
            'Unable set message dispatched for idempotency.'
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.random')
    def test_dispatch_raises_set_message_dispatched_after_kinesis_put(self,
                                                                      mock_random,
                                                                      mock_time,
                                                                      mock_set_message_dispatched,
                                                                      mock_send_next_event_for_dispatch,
                                                                      mock_start_retries,
                                                                      mock_queue_error):
        mock_random.uniform.return_value = 1.0
        mock_time.time.return_value = 1.0
        mock_set_message_dispatched.side_effect = Exception()
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        mock_send_next_event_for_dispatch.assert_called_with(
            instance,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "c", "current_state": '
            '"a", "machine_name": "foo", "max_retries": 5, "metrics": "m", "retries": 0, "steps": 1000, "stream": '
            '"s", "table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            'b',
            delay=0,
            primary=True,
            recovering=False
        )
        mock_start_retries.assert_called_with(
            instance,
            2.0,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "e", "current_state": '
            '"s", "machine_name": "foo", "metrics": "m", "retries": 1, "steps": 999, "stream": '
            '"s", "table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            primary=True,
            recovering=False
        )
        mock_queue_error.assert_called_with(
            'retry',
            'More retries allowed (retry=1, max=5). Retrying.'
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    def test_dispatch_fails_store_checkpoint_after_kinesis_put(self,
                                                               mock_stop_retries,
                                                               mock_time,
                                                               mock_store_checkpoint,
                                                               mock_set_message_dispatched,
                                                               mock_send_next_event_for_dispatch,
                                                               mock_queue_error):
        mock_time.time.return_value = 1.0
        mock_store_checkpoint.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        mock_set_message_dispatched.assert_called_with(
            'b',
            999,
            0,
            primary=False
        )
        mock_send_next_event_for_dispatch.assert_called_with(
            instance,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "c", "current_state": '
            '"a", "machine_name": "foo", "max_retries": 5, "metrics": "m", "retries": 0, "steps": 1000, "stream": '
            '"s", "table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            'b',
            delay=0,
            primary=True,
            recovering=False
        )
        mock_store_checkpoint.assert_called_with(
            instance,
            '{"put": "record"}',
            primary=False
        )
        mock_queue_error.assert_called_with(
            'error',
            'Unable to save last sent data (primary=False).',
            exc_info=True
        )
        mock_stop_retries.assert_called_with(
            instance,
            primary=True
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    @mock.patch('aws_lambda_fsm.fsm.set_message_dispatched')
    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    def test_dispatch_fails_stop_retries_after_kinesis_put(self,
                                                           mock_stop_retries,
                                                           mock_time,
                                                           mock_store_checkpoint,
                                                           mock_set_message_dispatched,
                                                           mock_send_next_event_for_dispatch,
                                                           mock_queue_error):
        mock_time.time.return_value = 1.0
        mock_stop_retries.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._dispatch(mock_send_next_event_for_dispatch)
        mock_set_message_dispatched.assert_called_with(
            'b',
            999,
            0,
            primary=False
        )
        mock_send_next_event_for_dispatch.assert_called_with(
            instance,
            '{"system_context": {"additional_delay_seconds": 0, '
            '"correlation_id": "b", "current_event": "c", "current_state": '
            '"a", "machine_name": "foo", "max_retries": 5, "metrics": "m", "retries": 0, "steps": 1000, "stream": '
            '"s", "table": "t", "topic": "z"}, "user_context": {}, "version": "0.1"}',
            'b',
            delay=0,
            primary=True,
            recovering=False
        )
        mock_store_checkpoint.assert_called_with(
            instance,
            '{"put": "record"}',
            primary=True
        )
        mock_queue_error.assert_called_with(
            'error',
            'Unable to terminate retries (primary=False).',
            exc_info=True
        )
        mock_stop_retries.assert_called_with(
            instance,
            primary=False
        )


class TestDispatchExclusiveLock(TestFsmBase):

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.acquire_lease')
    @mock.patch('aws_lambda_fsm.fsm.release_lease')
    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.Context._dispatch_and_retry')
    def test_lease_True(self,
                        mock_dispatch_and_retry,
                        mock_queue_error,
                        mock_release_lease,
                        mock_acquire_lease,
                        mock_uuid):
        mock_uuid.uuid4.return_value.hex = 'bobloblaw'
        mock_acquire_lease.return_value = 'foobar'
        mock_release_lease.return_value = 'foobar'
        instance = Context('name')
        instance.dispatch('event', {'foo': 'bar'})
        mock_acquire_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            primary=True,
            timeout=300
        )
        mock_release_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            'foobar',
            primary=True
        )
        self.assertFalse(mock_queue_error.called)
        mock_dispatch_and_retry.assert_called_with(
            'event',
            {'foo': 'bar'}
        )

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.acquire_lease')
    @mock.patch('aws_lambda_fsm.fsm.release_lease')
    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.Context._retry')
    def test_lease_False(self,
                         mock_retry,
                         mock_queue_error,
                         mock_release_lease,
                         mock_acquire_lease,
                         mock_uuid):
        mock_uuid.uuid4.return_value.hex = 'bobloblaw'
        mock_acquire_lease.return_value = False
        mock_release_lease.return_value = False
        instance = Context('name')
        instance.dispatch('event', {'foo': 'bar'})
        mock_acquire_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            primary=True,
            timeout=300
        )
        mock_release_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            False,
            primary=True
        )
        self.assertEqual(
            [
                mock.call('cache', 'Could not acquire lease. Retrying.'),
                mock.call('cache', 'Could not release lease.')
            ],
            mock_queue_error.mock_calls
        )
        mock_retry.assert_called_with(
            {'foo': 'bar'}
        )

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.acquire_lease')
    @mock.patch('aws_lambda_fsm.fsm.release_lease')
    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.Context._retry')
    def test_lease_0(self,
                     mock_retry,
                     mock_queue_error,
                     mock_release_lease,
                     mock_acquire_lease,
                     mock_uuid):
        mock_uuid.uuid4.return_value.hex = 'bobloblaw'
        mock_acquire_lease.return_value = 0
        mock_release_lease.return_value = 0
        instance = Context('name')
        instance.dispatch('event', {'foo': 'bar'})
        mock_acquire_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            primary=False,
            timeout=300
        )
        mock_release_lease.assert_called_with(
            'bobloblaw',
            0,
            0,
            False,
            primary=False
        )
        self.assertEqual(
            [
                mock.call('cache', 'System error acquiring primary=True lease.'),
                mock.call('cache', 'Could not acquire lease. Retrying.'),
                mock.call('cache', 'Could not release lease.')
            ],
            mock_queue_error.mock_calls
        )
        mock_retry.assert_called_with(
            {'foo': 'bar'}
        )

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.acquire_lease')
    @mock.patch('aws_lambda_fsm.fsm.release_lease')
    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.Context._retry')
    def test_lease_fence_token_int(self,
                                   mock_retry,
                                   mock_queue_error,
                                   mock_release_lease,
                                   mock_acquire_lease,
                                   mock_uuid):
        mock_uuid.uuid4.return_value.hex = 'bobloblaw'
        # equivalent to long in Python 2
        mock_acquire_lease.return_value = int(500)
        mock_release_lease.return_value = False
        instance = Context('name')
        obj = {'foo': 'bar'}
        instance.dispatch('event', obj)
        self.assertTrue('fence_token' in obj,
                        "Fence token should be set on the object if it's a numeric type.")
        self.assertEqual(obj['fence_token'], int(500))


class TestContextPrimarySecondary(TestFsmBase):

    def _instance(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance(
            'foo',
            initial_state_name='pseudo_init',
            initial_system_context={'correlation_id': 'foobar'}
        )
        return instance

    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    def test_store_checkpoint_happy_path(self,
                                         mock_store_checkpoint):
        instance = self._instance()
        instance._store_checkpoint({'sent': {'yep': 'sent'}})
        mock_store_checkpoint.assert_called_with(
            {},
            '{"yep": "sent"}',
            primary=True
        )

    @mock.patch('aws_lambda_fsm.fsm.store_checkpoint')
    def test_store_checkpoint_unhappy_path(self,
                                           mock_store_checkpoint):
        mock_store_checkpoint.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._instance()
        instance._store_checkpoint({'sent': {'yep': 'sent'}})
        mock_store_checkpoint.assert_called_with(
            {},
            '{"yep": "sent"}',
            primary=False
        )

    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.random')
    def test_start_retries_unhappy_path(self,
                                        mock_random,
                                        mock_time,
                                        mock_start_retries):
        mock_random.uniform.return_value = 1.0
        mock_start_retries.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        mock_time.time.return_value = 1.0
        instance = self._instance()
        instance._start_retries({'system_context': {'retries': 1}, 'user_context': {}}, {})
        mock_start_retries.assert_called_with(
            {},
            2.0,
            '{"system_context": {"retries": 1}, "user_context": {}}',
            primary=False,
            recovering=False
        )

    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.random')
    def test_start_retries_happy_path(self,
                                      mock_random,
                                      mock_time,
                                      mock_start_retries):
        mock_random.uniform.return_value = 1.0
        mock_time.time.return_value = 1.0
        instance = self._instance()
        instance._start_retries({'system_context': {'retries': 1}, 'user_context': {}}, {})
        mock_start_retries.assert_called_with(
            {},
            2.0,
            '{"system_context": {"retries": 1}, "user_context": {}}',
            primary=True,
            recovering=False
        )

    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.time')
    def test_stop_retries_happy_path(self,
                                     mock_time,
                                     mock_stop_retries):
        mock_time.time.return_value = 1.0
        instance = self._instance()
        instance._stop_retries({'source': 'dynamodb_retry'})
        mock_stop_retries.assert_called_with(
            {},
            primary=True
        )

    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.time')
    def test_stop_retries_unhappy_path(self,
                                       mock_time,
                                       mock_stop_retries):
        mock_stop_retries.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        mock_time.time.return_value = 1.0
        instance = self._instance()
        instance._stop_retries({'source': 'dynamodb_retry'})
        mock_stop_retries.assert_called_with(
            {},
            primary=False
        )

    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    def test_send_next_event_for_dispatch_happy_path(self,
                                                     mock_send_next_event_for_dispatch):
        instance = self._instance()
        instance._send_next_event_for_dispatch('a', {})
        mock_send_next_event_for_dispatch.assert_called_with(
            {},
            'a',
            'foobar',
            delay=0,
            primary=True,
            recovering=False
        )

    @mock.patch('aws_lambda_fsm.fsm.send_next_event_for_dispatch')
    def test_send_next_event_for_dispatch_unhappy_path(self,
                                                       mock_send_next_event_for_dispatch):
        mock_send_next_event_for_dispatch.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._instance()
        # raise out the errors
        self.assertRaises(ClientError, instance._send_next_event_for_dispatch, 'a', {}, recovering=True)
        mock_send_next_event_for_dispatch.assert_called_with(
            {},
            'a',
            'foobar',
            delay=0,
            primary=False,
            recovering=True
        )


class TestRetry(TestFsmBase):
    def _dispatch_client_error(self,
                               mock_time,
                               mock_uuid,
                               retries=0):
        mock_time.time.return_value = 100.
        mock_uuid.uuid4.return_value.hex = 'foobar'
        config._config = {'some/fsm.yaml': {'machines': []}}
        config_dict = copy.deepcopy(self.CONFIG_DICT)
        config_dict['machines'][1]['states'][0]['do_action'] = 'tests.aws_lambda_fsm.test_fsm.ErrorAction'
        payload = {
            "system_context": {
                "machine_name": "m",
                "current_state": "s",
                "current_event": "e",
                "correlation_id": "b",
                "steps": 999,
                "retries": retries},
            "user_context": {}
        }
        fsm = FSM(config_dict=config_dict)
        instance = fsm.create_FSM_instance(
            'foo',
            initial_state_name='pseudo_init',
            initial_system_context=payload['system_context'],
            initial_user_context=payload['user_context']
        )
        instance._dispatch_and_retry(
            'pseudo_init',
            {'payload': json.dumps(payload, **json_dumps_additional_kwargs()), 'source': 'dynamodb_retry'}
        )
        self.assertEqual(instance.retries, retries)
        return instance

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.random')
    def test_dispatch_exception_retries(self,
                                        mock_random,
                                        mock_time,
                                        mock_uuid,
                                        mock_start_retries,
                                        mock_queue_error):
        mock_random.uniform.return_value = 1.0
        instance = self._dispatch_client_error(mock_time, mock_uuid)
        mock_start_retries.assert_called_with(
            instance,
            101.0,
            '{"system_context": {"correlation_id": "b", "current_event": "e", "current_state": '
            '"s", "machine_name": "m", "retries": 1, "steps": 999}, "user_context": {}}',
            primary=True,
            recovering=False
        )
        mock_queue_error.assert_called_with(
            'retry',
            'More retries allowed (retry=1, max=5). Retrying.'
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.start_retries')
    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.time')
    @mock.patch('aws_lambda_fsm.fsm.random')
    def test_dispatch_exception_retries_raises(self,
                                               mock_random,
                                               mock_time,
                                               mock_uuid,
                                               mock_start_retries,
                                               mock_queue_error):
        mock_random.uniform.return_value = 1.0
        mock_start_retries.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._dispatch_client_error(mock_time, mock_uuid)
        payload = '{"system_context": {"correlation_id": "b", "current_event": "e", "current_state": ' \
                  '"s", "machine_name": "m", "retries": 1, "steps": 999}, "user_context": {}}'
        mock_start_retries.assert_called_with(
            instance,
            101.0,
            payload,
            primary=False,
            recovering=True
        )
        mock_queue_error.assert_called_with(
            'error',
            'Unable to save last payload for retry (primary=False).',
            exc_info=True
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.time')
    def test_dispatch_exception_max_retries(self,
                                            mock_time,
                                            mock_uuid,
                                            mock_stop_retries,
                                            mock_queue_error):
        instance = self._dispatch_client_error(mock_time, mock_uuid, retries=999)
        mock_stop_retries.assert_called_with(
            instance,
            primary=True
        )
        mock_queue_error(
            'fatal'
        )

    @mock.patch('aws_lambda_fsm.fsm.Context._queue_error')
    @mock.patch('aws_lambda_fsm.fsm.stop_retries')
    @mock.patch('aws_lambda_fsm.fsm.uuid')
    @mock.patch('aws_lambda_fsm.fsm.time')
    def test_dispatch_exception_max_retries_raises(self,
                                                   mock_time,
                                                   mock_uuid,
                                                   mock_stop_retries,
                                                   mock_queue_error):
        mock_stop_retries.side_effect = ClientError({'Error': {'Code': 404}}, 'test')
        instance = self._dispatch_client_error(mock_time, mock_uuid, retries=999)
        mock_stop_retries.assert_called_with(
            instance,
            primary=False
        )
        mock_queue_error.assert_called_with(
            'error',
            'Unable to terminate retries (primary=False).',
            exc_info=True
        )


class TestInitialize(TestFsmBase):

    @mock.patch('aws_lambda_fsm.fsm.Context.dispatch')
    def test_initialize(self, mock_dispatch):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo')
        instance.initialize()
        mock_dispatch.assert_called_with('foo', 'pseudo_init', None)


class TestProperties(TestFsmBase):

    @mock.patch('aws_lambda_fsm.fsm.uuid')
    def test_correlation_id_not_set(self, mock_uuid):
        mock_uuid.uuid4.return_value.hex = 'foobar'
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo')
        self.assertEqual('foobar', instance.correlation_id)

    def test_correlation_id(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={'correlation_id': 'barfoo'})
        self.assertEqual('barfoo', instance.correlation_id)

    def test_additional_delay_seconds_defaults_to_0(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={})
        self.assertEqual(0, instance.additional_delay_seconds)

    def test_additional_delay_seconds(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={'additional_delay_seconds': 999})
        self.assertEqual(999, instance.additional_delay_seconds)

    def test_lookup_property_uses_context(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={"key": "context"})
        self.assertEqual("context", instance._lookup_property("key", "setting", "default"))

    @mock.patch('aws_lambda_fsm.fsm.settings')
    def test_lookup_property_uses_settings(self, mock_settings):
        mock_settings.setting = "foobar"
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={})
        self.assertEqual("foobar", instance._lookup_property("key", "setting", "default"))

    def test_lookup_property_uses_constant(self):
        config._config = {'some/fsm.yaml': {'machines': []}}
        fsm = FSM(config_dict=self.CONFIG_DICT)
        instance = fsm.create_FSM_instance('foo', initial_system_context={})
        self.assertEqual("default", instance._lookup_property("key", "setting", "default"))
