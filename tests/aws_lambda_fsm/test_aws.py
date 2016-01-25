# system imports
import unittest

# library imports
import mock
from botocore.exceptions import ClientError

# application imports
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import ENDPOINTS
from aws_lambda_fsm.aws import get_connection
from aws_lambda_fsm.aws import retriable_entities
from aws_lambda_fsm.aws import store_checkpoint
from aws_lambda_fsm.aws import store_environment
from aws_lambda_fsm.aws import load_environment
from aws_lambda_fsm.aws import start_retries
from aws_lambda_fsm.aws import stop_retries
from aws_lambda_fsm.aws import send_next_event_for_dispatch
from aws_lambda_fsm.aws import send_next_events_for_dispatch
from aws_lambda_fsm.aws import set_message_dispatched
from aws_lambda_fsm.aws import get_message_dispatched
from aws_lambda_fsm.aws import increment_error_counters
from aws_lambda_fsm.aws import get_primary_stream_source
from aws_lambda_fsm.aws import get_secondary_stream_source
from aws_lambda_fsm.aws import get_primary_environment_source
from aws_lambda_fsm.aws import get_secondary_environment_source
from aws_lambda_fsm.aws import get_primary_checkpoint_source
from aws_lambda_fsm.aws import get_secondary_checkpoint_source
from aws_lambda_fsm.aws import _local
from aws_lambda_fsm.aws import _get_service_connection
from aws_lambda_fsm.aws import ChaosConnection
from aws_lambda_fsm.aws import NoOpCache
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.aws import cache_get
from aws_lambda_fsm.aws import cache_add
from aws_lambda_fsm.aws import cache_delete


class Connection(object):
    _method_to_api_mapping = {'find_things': 'FindThingsApi'}

    def find_things(self):
        return 1


def _get_test_arn(service):
    return ':'.join(
        ['arn', 'aws', service, 'testing', '1234567890', 'resourcetype/resourcename']
    )


class TestAws(unittest.TestCase):

    def test_chaos_0(self):
        connection = Connection()
        connection = ChaosConnection('kinesis', connection, chaos={'dynamodb': {Exception(): 1.0}})
        ret = connection.find_things()
        self.assertEqual(1, ret)

    def test_chaos_0_explicit(self):
        connection = Connection()
        connection = ChaosConnection('kinesis', connection, chaos={'kinesis': {Exception(): 0.0}})
        ret = connection.find_things()
        self.assertEqual(1, ret)

    def test_chaos_100_raise(self):
        connection = Connection()
        connection = ChaosConnection('kinesis', connection, chaos={'kinesis': {Exception(): 1.0}})
        self.assertRaises(Exception, connection.find_things)

    def test_chaos_100_return(self):
        connection = Connection()
        connection = ChaosConnection('kinesis', connection, chaos={'kinesis': {'zap': 1.0}})
        ret = connection.find_things()
        self.assertEqual('zap', ret)

    ##################################################
    # Connection Functions
    ##################################################

    # get_arn_from_arn_string

    def test_get_arn_from_arn_string(self):
        arn = get_arn_from_arn_string("a:b:c:d:e:f:g:h")
        self.assertEqual('a', arn.arn)
        self.assertEqual('b', arn.partition)
        self.assertEqual('c', arn.service)
        self.assertEqual('d', arn.region_name)
        self.assertEqual('e', arn.account_id)
        self.assertEqual('f:g:h', arn.resource)

    def test_get_arn_from_arn_string_not_long_enough(self):
        arn = get_arn_from_arn_string("a:b:c")
        self.assertEqual('a', arn.arn)
        self.assertEqual('b', arn.partition)
        self.assertEqual('c', arn.service)
        self.assertEqual(None, arn.region_name)
        self.assertEqual(None, arn.account_id)
        self.assertEqual(None, arn.resource)

    def test_get_arn_from_arn_string_no_string_at_all(self):
        arn = get_arn_from_arn_string(None)
        self.assertEqual(None, arn.arn)
        self.assertEqual(None, arn.partition)
        self.assertEqual(None, arn.service)
        self.assertEqual(None, arn.region_name)
        self.assertEqual(None, arn.account_id)
        self.assertEqual(None, arn.resource)

    # _get_service_connection

    @mock.patch('aws_lambda_fsm.aws._get_connection_info')
    def test_get_service_connection_sets_local_var(self,
                                                   mock_get_connection_info):
        mock_get_connection_info.return_value = 'kinesis', 'testing', 'http://localhost:1234'
        _local.kinesis_testing_connection = None
        conn = _get_service_connection(_get_test_arn(AWS.KINESIS))
        self.assertIsNotNone(conn)
        self.assertIsNotNone(_local.kinesis_testing_connection)

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_service_connection_memcache_exists(self, mock_settings):
        mock_settings.ENDPOINTS = {
            AWS.ELASTICACHE: {
                ENDPOINTS.ENDPOINT_URL: 'foobar:1234'
            }
        }
        _local.elasticache_testing_connection = None
        conn = _get_service_connection(_get_test_arn(AWS.ELASTICACHE))
        self.assertIsNotNone(conn)
        self.assertIsNotNone(_local.elasticache_testing_connection)

    @mock.patch('aws_lambda_fsm.aws._get_connection_info')
    def test_get_service_connection_memcache_not_exists(self,
                                                        mock_get_connection_info):
        mock_get_connection_info.return_value = 'elasticache', 'testing', None
        _local.elasticache_testing_connection = None
        conn = _get_service_connection(_get_test_arn(AWS.ELASTICACHE))
        self.assertIsNone(conn)
        self.assertIsNone(_local.elasticache_testing_connection)

    # get_connection

    @mock.patch('aws_lambda_fsm.aws._get_service_connection')
    def test_get_kinesis_connection(self,
                                    mock_get_service_connection):
        _local.kinesis_connection = None
        conn = get_connection(_get_test_arn(AWS.KINESIS))
        self.assertIsNotNone(conn)
        mock_get_service_connection.assert_called_with(_get_test_arn(AWS.KINESIS))

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_kinesis_connection_settings(self,
                                             mock_settings):
        mock_settings.USE_KINESIS = False
        _local.kinesis_connection = None
        conn = get_connection(_get_test_arn(AWS.KINESIS))
        self.assertIsNone(conn)

    @mock.patch('aws_lambda_fsm.aws._get_service_connection')
    def test_get_memcache_connection(self,
                                     mock_get_service_connection):
        _local.elasticache_testing_connection = None
        conn = get_connection(_get_test_arn(AWS.ELASTICACHE))
        self.assertIsNotNone(conn)
        mock_get_service_connection.assert_called_with(_get_test_arn(AWS.ELASTICACHE))

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_memcache_connection_settings(self,
                                              mock_settings):
        mock_settings.USE_ELASTICACHE = False
        _local.elasticache_testing_connection = None
        conn = get_connection(_get_test_arn(AWS.ELASTICACHE))
        self.assertTrue(isinstance(conn, NoOpCache))

    ##################################################
    # Functions
    ##################################################

    # increment_error_counters

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.datetime')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_increment_error_counter(self,
                                     mock_settings,
                                     mock_datetime,
                                     mock_get_connection):
        mock_settings.PRIMARY_METRICS_SOURCE = _get_test_arn(AWS.CLOUDWATCH)
        mock_datetime.datetime.utcnow.return_value = 'now'
        increment_error_counters({'a': 98, 'b': 99}, {'d': 'e'})
        mock_get_connection.return_value.put_metric_data.assert_called_with(
            Namespace='resourcetype/resourcename',
            MetricData=[
                {
                    'Timestamp': 'now',
                    'Dimensions': [
                        {'Name': 'd', 'Value': 'e'}
                    ],
                    'Value': 98,
                    'MetricName': 'a'
                },
                {
                    'Timestamp': 'now',
                    'Dimensions': [
                        {'Name': 'd', 'Value': 'e'}
                    ],
                    'Value': 99,
                    'MetricName': 'b'
                }
            ]
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_increment_error_counter_no_connection(self,
                                                   mock_get_connection):
        mock_get_connection.return_value = None
        ret = increment_error_counters([('b', 99)], {'d': 'e'})
        self.assertIsNone(ret)

    # get_primary_stream_source
    # get_secondary_stream_source

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_primary_stream_source(self,
                                       mock_settings):
        mock_settings.PRIMARY_STREAM_SOURCE = 'foo'
        self.assertEqual('foo', get_primary_stream_source())

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_secondary_stream_source(self,
                                         mock_settings):
        mock_settings.SECONDARY_STREAM_SOURCE = 'bar'
        self.assertEqual('bar', get_secondary_stream_source())

    # get_primary_environment_source
    # get_secondary_environment_source

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_primary_environment_source(self,
                                            mock_settings):
        mock_settings.PRIMARY_ENVIRONMENT_SOURCE = 'foo'
        self.assertEqual('foo', get_primary_environment_source())

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_secondary_environment_source(self,
                                              mock_settings):
        mock_settings.SECONDARY_ENVIRONMENT_SOURCE = 'bar'
        self.assertEqual('bar', get_secondary_environment_source())

    # get_primary_checkpoint_source
    # get_secondary_checkpoint_source

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_primary_checkpoint_source(self,
                                           mock_settings):
        mock_settings.PRIMARY_CHECKPOINT_SOURCE = 'foo'
        self.assertEqual('foo', get_primary_checkpoint_source())

    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_get_secondary_checkpoint_source(self,
                                             mock_settings):
        mock_settings.SECONDARY_CHECKPOINT_SOURCE = 'bar'
        self.assertEqual('bar', get_secondary_checkpoint_source())

    # store_environment

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_environment_source')
    @mock.patch('aws_lambda_fsm.aws.uuid')
    def test_store_environment_dynamodb(self,
                                        mock_uuid,
                                        mock_get_primary_environment_source,
                                        mock_get_connection):
        mock_uuid.uuid4.return_value.hex = 'guid'
        mock_context = mock.Mock()
        mock_get_primary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        store_environment(mock_context, {'a': 'b'})
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'environment': {'S': '{"a": "b"}'}, 'guid': {'S': 'guid'}},
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_secondary_environment_source')
    @mock.patch('aws_lambda_fsm.aws.uuid')
    def test_store_environment_dynamodb_secondary(self,
                                                  mock_uuid,
                                                  get_secondary_environment_source,
                                                  mock_get_connection):
        mock_uuid.uuid4.return_value.hex = 'guid'
        mock_context = mock.Mock()
        get_secondary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        store_environment(mock_context, {'a': 'b'}, primary=False)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'environment': {'S': '{"a": "b"}'}, 'guid': {'S': 'guid'}},
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_environment_source')
    @mock.patch('aws_lambda_fsm.aws.uuid')
    def test_store_environment_dynamodb_disabled(self,
                                                 mock_uuid,
                                                 mock_get_primary_environment_source,
                                                 mock_get_connection):
        mock_uuid.uuid4.return_value.hex = 'guid'
        mock_context = mock.Mock()
        mock_get_primary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value = None
        store_environment(mock_context, {'a': 'b'})

    # load_environment

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_environment_source')
    def test_load_environment_dynamodb(self,
                                       mock_get_primary_environment_source,
                                       mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.get_item.return_value = \
            {'Item': {'environment': {'S': '{"a": "b"}'}, 'guid': {'S': 'guid'}}}
        env = load_environment(mock_context, _get_test_arn(AWS.DYNAMODB) + ';' + 'guid')
        self.assertEqual({'a': 'b'}, env)
        mock_get_connection.return_value.get_item.assert_called_with(
            ConsistentRead=True, TableName='resourcename', Key={'guid': {'S': 'guid'}}
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_secondary_environment_source')
    def test_load_environment_dynamodb_secondary(self,
                                                 mock_get_secondary_environment_source,
                                                 mock_get_connection):
        mock_context = mock.Mock()
        mock_get_secondary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.get_item.return_value = \
            {'Item': {'environment': {'S': '{"a": "b"}'}, 'guid': {'S': 'guid'}}}
        env = load_environment(mock_context, _get_test_arn(AWS.DYNAMODB) + ';' + 'guid', primary=False)
        self.assertEqual({'a': 'b'}, env)
        mock_get_connection.return_value.get_item.assert_called_with(
            ConsistentRead=True, TableName='resourcename', Key={'guid': {'S': 'guid'}}
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_environment_source')
    def test_load_environment_dynamodb_disabled(self,
                                                mock_get_primary_environment_source,
                                                mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_environment_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value = None
        load_environment(mock_context, _get_test_arn(AWS.DYNAMODB) + ';' + 'guid')

    # send_next_event_for_dispatch

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_event_for_dispatch_kinesis(self,
                                                  mock_get_primary_stream_source,
                                                  mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.KINESIS)
        send_next_event_for_dispatch(mock_context, 'c', 'd')
        mock_get_connection.return_value.put_record.assert_called_with(
            PartitionKey='d',
            Data='c',
            StreamName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_secondary_stream_source')
    def test_send_next_event_for_dispatch_kinesis_secondary(self,
                                                            mock_get_secondary_stream_source,
                                                            mock_get_connection):
        mock_context = mock.Mock()
        mock_get_secondary_stream_source.return_value = _get_test_arn(AWS.KINESIS)
        send_next_event_for_dispatch(mock_context, 'c', 'd', primary=False)
        mock_get_connection.return_value.put_record.assert_called_with(
            PartitionKey='d',
            Data='c',
            StreamName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    @mock.patch('aws_lambda_fsm.aws.time')
    def test_send_next_event_for_dispatch_dynamodb(self,
                                                   mock_time,
                                                   mock_get_primary_stream_source,
                                                   mock_get_connection):
        mock_context = mock.Mock()
        mock_time.time.return_value = 1234.0
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.DYNAMODB)
        send_next_event_for_dispatch(mock_context, 'c', 'd')
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'timestamp': {'N': '1234'}, 'correlation_id': {'S': 'd'}, 'payload': {'S': 'c'}},
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_event_for_dispatch_sns(self,
                                              mock_get_primary_stream_source,
                                              mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.SNS)
        send_next_event_for_dispatch(mock_context, 'c', 'd')
        mock_get_connection.return_value.publish.assert_called_with(
            Message='{"default": "c"}',
            TopicArn=_get_test_arn(AWS.SNS)
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_event_for_dispatch_sqs(self,
                                              mock_get_primary_stream_source,
                                              mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.SQS)
        send_next_event_for_dispatch(mock_context, 'c', 'd')
        mock_get_connection.return_value.send_message.assert_called_with(
            QueueUrl='https://sqs.testing.amazonaws.com/1234567890/resourcetype/resourcename',
            DelaySeconds=0,
            MessageBody='c'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_events_for_dispatch_kinesis(self,
                                                   mock_get_primary_stream_source,
                                                   mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.KINESIS)
        send_next_events_for_dispatch(mock_context, ['c', 'cc'], ['d', 'dd'])
        mock_get_connection.return_value.put_records.assert_called_with(
            Records=[{'PartitionKey': 'd', 'Data': 'c'}, {'PartitionKey': 'dd', 'Data': 'cc'}],
            StreamName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_secondary_stream_source')
    def test_send_next_events_for_dispatch_kinesis_secondary(self,
                                                             mock_get_secondary_stream_source,
                                                             mock_get_connection):
        mock_context = mock.Mock()
        mock_get_secondary_stream_source.return_value = _get_test_arn(AWS.KINESIS)
        send_next_events_for_dispatch(mock_context, ['c', 'cc'], ['d', 'dd'], primary=False)
        mock_get_connection.return_value.put_records.assert_called_with(
            Records=[{'PartitionKey': 'd', 'Data': 'c'}, {'PartitionKey': 'dd', 'Data': 'cc'}],
            StreamName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    @mock.patch('aws_lambda_fsm.aws.time')
    def test_send_next_events_for_dispatch_dynamodb(self,
                                                    mock_time,
                                                    mock_get_primary_stream_source,
                                                    mock_get_connection):
        mock_context = mock.Mock()
        mock_time.time.return_value = 1234.0
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.DYNAMODB)
        send_next_events_for_dispatch(mock_context, ['c', 'cc'], ['d', 'dd'])
        mock_get_connection.return_value.batch_write_item.assert_called_with(
            RequestItems={
                'resourcename': [
                    {'PutRequest': {'Item': {'timestamp': {'N': '1234'},
                                             'correlation_id': {'S': 'd'}, 'payload': {'S': 'c'}}}},
                    {'PutRequest': {'Item': {'timestamp': {'N': '1234'},
                                             'correlation_id': {'S': 'dd'}, 'payload': {'S': 'cc'}}}}
                ]
            }
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_events_for_dispatch_sns(self,
                                               mock_get_primary_stream_source,
                                               mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.SNS)
        send_next_events_for_dispatch(mock_context, ['c', 'cc'], ['d', 'dd'])
        mock_get_connection.return_value.publish.assert_has_calls(
            [
                mock.call(Message='{"default": "c"}', TopicArn=_get_test_arn(AWS.SNS)),
                mock.call(Message='{"default": "cc"}', TopicArn=_get_test_arn(AWS.SNS))
            ], any_order=True
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_stream_source')
    def test_send_next_events_for_dispatch_sqs(self,
                                               mock_get_primary_stream_source,
                                               mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_stream_source.return_value = _get_test_arn(AWS.SQS)
        send_next_events_for_dispatch(mock_context, ['c', 'cc'], ['d', 'dd'])
        mock_get_connection.return_value.send_message_batch.assert_called_with(
            QueueUrl='https://sqs.testing.amazonaws.com/1234567890/resourcetype/resourcename',
            Entries=[{'DelaySeconds': 0, 'Id': 'd', 'MessageBody': 'c'},
                     {'DelaySeconds': 0, 'Id': 'dd', 'MessageBody': 'cc'}]
        )

    # retriable_entities

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_retriable_entities(self,
                                mock_get_connection):
        mock_get_connection.return_value.query.return_value = {
            'Items': [{'payload': {'S': 'a'}, 'correlation_id': {'S': 'b'}}]
        }
        items = retriable_entities(_get_test_arn(AWS.DYNAMODB), 'b', 'c')
        self.assertTrue(items)
        mock_get_connection.return_value.query.assert_called_with(
            TableName='resourcename',
            ConsistentRead=True,
            Limit=100,
            IndexName='b',
            KeyConditions={'partition': {'ComparisonOperator': 'EQ',
                                         'AttributeValueList': [{'N': '15'}]},
                           'run_at': {'ComparisonOperator': 'LT',
                                      'AttributeValueList': [{'N': 'c'}]}}
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_retriable_entities_no_connection(self,
                                              mock_get_connection):
        mock_get_connection.return_value = None
        iter = retriable_entities('a', 'b', 'c')
        self.assertEqual([], iter)

    # start_retries

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_start_retries_primary(self,
                                   mock_settings,
                                   mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        mock_settings.PRIMARY_RETRY_SOURCE = _get_test_arn(AWS.DYNAMODB)
        start_retries(mock_context, 'c', 'd', primary=True)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'partition': {'N': '15'},
                  'payload': {'S': 'd'},
                  'correlation_id': {'S': 'b'},
                  'run_at': {'N': 'c'}},
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_start_retries_secondary(self,
                                     mock_settings,
                                     mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        mock_settings.SECONDARY_RETRY_SOURCE = _get_test_arn(AWS.KINESIS)
        start_retries(mock_context, 'c', 'd', primary=False)
        mock_get_connection.return_value.put_record.assert_called_with(
            PartitionKey='b',
            Data='d',
            StreamName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_start_retries_sns(self,
                               mock_settings,
                               mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        mock_settings.PRIMARY_RETRY_SOURCE = _get_test_arn(AWS.SNS)
        start_retries(mock_context, 'c', 'd', primary=True)
        mock_get_connection.return_value.publish.assert_called_with(
            Message='{"default": "d"}',
            TopicArn=_get_test_arn(AWS.SNS)
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_start_retries_sqs(self,
                               mock_settings,
                               mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        mock_settings.PRIMARY_RETRY_SOURCE = _get_test_arn(AWS.SQS)
        start_retries(mock_context, 123, 'd', primary=True)
        mock_get_connection.return_value.send_message.assert_called_with(
            QueueUrl='https://sqs.testing.amazonaws.com/1234567890/resourcetype/resourcename',
            DelaySeconds=0,
            MessageBody='d'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_start_retries_no_connection(self,
                                         mock_get_connection):
        mock_context = mock.Mock()
        mock_get_connection.return_value = None
        ret = start_retries(mock_context, 'c', 'd')
        self.assertIsNone(ret)

    # stop_retries

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.settings')
    def test_stop_retries_primary(self,
                                  mock_settings,
                                  mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        mock_settings.PRIMARY_RETRY_SOURCE = _get_test_arn(AWS.DYNAMODB)
        stop_retries(mock_context, primary=True)
        mock_get_connection.return_value.delete_item.assert_called_with(
            Key={'partition': {'N': '15'},
                 'correlation_id': {'S': 'b'}},
            TableName='resourcename'
        )

    def test_stop_retries_secondary(self):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'b'
        stop_retries(mock_context, primary=False)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_stop_retries_no_connection(self,
                                        mock_get_connection):
        mock_context = mock.Mock()
        mock_get_connection.return_value = None
        ret = stop_retries(mock_context)
        self.assertIsNone(ret)

    # store_checkpoint

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_checkpoint_source')
    def test_store_checkpoint_kinesis(self,
                                      mock_get_primary_checkpoint_source,
                                      mock_get_connection):
        mock_context = mock.Mock()
        mock_get_primary_checkpoint_source.return_value = _get_test_arn(AWS.KINESIS)
        store_checkpoint(mock_context, 'd')
        self.assertFalse(mock_get_connection.return_value.put_record.called)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_secondary_checkpoint_source')
    def test_store_checkpoint_dynamodb(self,
                                       mock_get_secondary_stream_source,
                                       mock_get_connection):
        mock_context = mock.Mock()
        mock_context.correlation_id = 'c'
        mock_get_secondary_stream_source.return_value = _get_test_arn(AWS.DYNAMODB)
        store_checkpoint(mock_context, 'd', primary=False)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'partition': {'N': '3'},
                  'sent': {'S': 'd'},
                  'correlation_id': {'S': 'c'}},
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_store_checkpoint_no_connection(self,
                                            mock_get_connection):
        mock_context = mock.Mock()
        mock_get_connection.return_value = None
        ret = store_checkpoint(mock_context, 'c', 'd')
        self.assertIsNone(ret)

    # set_message_dispatched

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_set_message_dispatched_no_connection(self,
                                                  mock_get_connection):
        mock_get_connection.return_value = None
        ret = set_message_dispatched('a', 'b')
        self.assertFalse(ret)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_set_message_dispatched_memcache(self,
                                             mock_get_primary_cache_source,
                                             mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.CLOUDWATCH)
        ret = set_message_dispatched('a', 'b')
        self.assertTrue(ret)
        mock_get_connection.return_value.set.assert_called_with('a-b', True)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_set_message_dispatched_dynamodb(self,
                                             mock_get_primary_cache_source,
                                             mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        ret = set_message_dispatched('a', 'b')
        self.assertTrue(ret)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'ckey': {'S': 'a-b'}, 'value': {'BOOL': True}},
            TableName='resourcename'
        )

    # get_message_dispatched

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_get_message_dispatched_no_connection(self,
                                                  mock_get_connection):
        mock_get_connection.return_value = None
        ret = get_message_dispatched('a', 'b')
        self.assertFalse(ret)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_get_message_dispatched_memcache(self,
                                             mock_get_primary_cache_source,
                                             mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.CLOUDWATCH)
        mock_get_connection.return_value.get.return_value = 'foobar'
        ret = get_message_dispatched('a', 'b')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.get.assert_called_with('a-b')

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_get_message_dispatched_dynamodb(self,
                                             mock_get_primary_cache_source,
                                             mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.get_item.return_value = {'Item': {'value': {'BOOL': 'foobar'}}}
        ret = get_message_dispatched('a', 'b')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.get_item.assert_called_with(
            ConsistentRead=True,
            TableName='resourcename',
            Key={'ckey': {'S': 'a-b'}}
        )

    # cache_get

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_cache_get_no_connection(self,
                                     mock_get_connection):
        mock_get_connection.return_value = None
        ret = cache_get('a')
        self.assertFalse(ret)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_get_memcache(self,
                                mock_get_primary_cache_source,
                                mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.CLOUDWATCH)
        mock_get_connection.return_value.get.return_value = 'foobar'
        ret = cache_get('a')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.get.assert_called_with('a')

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_get_dynamodb(self,
                                mock_get_primary_cache_source,
                                mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.get_item.return_value = {'Item': {'value': {'S': 'foobar'}}}
        ret = cache_get('a')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.get_item.assert_called_with(
            ConsistentRead=True,
            TableName='resourcename',
            Key={'ckey': {'S': 'a'}}
        )

    # cache_add

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_cache_add_no_connection(self,
                                     mock_get_connection):
        mock_get_connection.return_value = None
        ret = cache_add('a', 'b')
        self.assertFalse(ret)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_add_memcache(self,
                                mock_get_primary_cache_source,
                                mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.CLOUDWATCH)
        mock_get_connection.return_value.add.return_value = 'foobar'
        ret = cache_add('a', 'b')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.add.assert_called_with('a', 'b')

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_add_dynamodb(self,
                                mock_get_primary_cache_source,
                                mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.put_item.return_value = 'foobar'
        ret = cache_add('a', 'b')
        self.assertEqual(True, ret)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'ckey': {'S': 'a'}, 'value': {'S': 'b'}},
            ConditionExpression='attribute_not_exists(ckey)',
            TableName='resourcename'
        )

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_add_dynamodb_exists(self,
                                       mock_get_primary_cache_source,
                                       mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.put_item.side_effect = \
            ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}},
                        'Operation')
        ret = cache_add('a', 'b')
        self.assertEqual(False, ret)
        mock_get_connection.return_value.put_item.assert_called_with(
            Item={'ckey': {'S': 'a'}, 'value': {'S': 'b'}},
            ConditionExpression='attribute_not_exists(ckey)',
            TableName='resourcename'
        )

    # cache_delete

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    def test_cache_delete_no_connection(self,
                                        mock_get_connection):
        mock_get_connection.return_value = None
        ret = cache_delete('a')
        self.assertFalse(ret)

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_deleta_memcache(self,
                                   mock_get_primary_cache_source,
                                   mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.CLOUDWATCH)
        mock_get_connection.return_value.delete.return_value = 'foobar'
        ret = cache_delete('a', 'b')
        self.assertEqual('foobar', ret)
        mock_get_connection.return_value.delete.assert_called_with('a')

    @mock.patch('aws_lambda_fsm.aws.get_connection')
    @mock.patch('aws_lambda_fsm.aws.get_primary_cache_source')
    def test_cache_delete_dynamodb(self,
                                   mock_get_primary_cache_source,
                                   mock_get_connection):
        mock_get_primary_cache_source.return_value = _get_test_arn(AWS.DYNAMODB)
        mock_get_connection.return_value.delete_item.return_value = 'foobar'
        ret = cache_delete('a', 'b')
        self.assertEqual(True, ret)
        mock_get_connection.return_value.delete_item.assert_called_with(
            TableName='resourcename',
            Key={'ckey': {'S': 'a'}}
        )
