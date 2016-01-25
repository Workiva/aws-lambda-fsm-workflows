# system imports
from threading import RLock
import logging
import time
import datetime
import hashlib
import random
import os
import uuid
import json
from collections import namedtuple

# library imports
import boto3
from botocore.exceptions import ClientError

# application imports
from aws_lambda_fsm.constants import ENVIRONMENT_DATA
from aws_lambda_fsm.constants import RECOVERY_DATA
from aws_lambda_fsm.constants import CACHE_DATA
from aws_lambda_fsm.constants import STREAM_DATA
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.constants import AWS_KINESIS
from aws_lambda_fsm.constants import AWS_CLOUDWATCH
from aws_lambda_fsm.constants import AWS_SQS
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import ENDPOINTS
from aws_lambda_fsm.constants import ENVIRONMENT
from aws_lambda_fsm.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class Object(object):
    pass


_local = Object()
_lock = RLock()

TRACE = 5


class ChaosFunction(object):
    """
    A callable class that raises an exception or returns a fixed (error) value.
    Used by ChaosConnection when a error is to be returned.
    """

    def __init__(self, exception_or_return):
        self.exception_or_return = exception_or_return

    def __call__(self, *args, **kwargs):
        if isinstance(self.exception_or_return, Exception):
            raise self.exception_or_return
        else:
            return self.exception_or_return


class ChaosConnection(object):
    """
    A wrapper for a memcache.Client or a botocore.Client that raises an exception
    or returns a fixed (error) value.
    """

    def __init__(self, service, client, chaos=None):
        chaos = chaos or getattr(settings, 'AWS_CHAOS', {})
        self.service = service
        self.client = client
        self.chaos = chaos.get(service, {})
        if os.environ.get('DISABLE_AWS_CHAOS'):
            self.chaos = {}  # pragma: no cover

    def __getattr__(self, attr):
        original_attr = getattr(self.client, attr)
        if self.chaos:
            if callable(original_attr):
                for exception_or_return, percentage in self.chaos.iteritems():
                    if random.uniform(0.0, 1.0) < percentage:
                        return ChaosFunction(exception_or_return)
        return original_attr


Arn = namedtuple('Arn', ['arn', 'partition', 'service', 'region_name', 'account_id', 'resource'])


def get_arn_from_arn_string(arn):
    """
    Parses an ARN like "arn:partition:kinesis:region:account:resource" into
    a namedtuple that is more friendly to use.

    :param arn: an ARN like "arn:partition:kinesis:region:account:resource"
    :return: a instance of Arn nametuple
    """
    if arn:
        parts = arn.split(':', 5)
        if len(parts) < 6:
            parts += [None] * (6-len(parts))
        return Arn(*parts)
    else:
        return Arn(None, None, None, None, None, None)


def _get_connection_info(service, region_name):
    """
    Returns the service, region_name and endpoint_url to use when creating
    a boto3 connection. In settings.ENDPOINTS it is possible to override
    the default boto3 endpoints to talk to local instances of kinesis,
    dynamodb etc.

    :param service: an AWS service like "kinesis", or "dynamodb"
    :param region_name: an AWS region like "eu-west-1"
    :return: a tuple of service, region_name, and and possibly endpoint url
      like "http://localhost:1234"
    """
    endpoint_url = \
        getattr(settings, 'ENDPOINTS', {}).get(service, {}).get(ENDPOINTS.ENDPOINT_URL) or \
        os.environ.get(service.upper() + '_URI')
    region_name = 'testing' if endpoint_url else region_name
    return service, region_name, endpoint_url


def _service_enabled(service):
    """
    Returns True if the service is enabled, and False otherwise. Provides
    a simple short circuit for local testing when there are, for instance,
    not local cloudwatch stubs to talk to.

    :param service: an AWS service like "kinesis", or "dynamodb"
    :return: True if the service is enabled, and False otherwise
    """
    attr = 'USE_' + service.upper()
    return getattr(settings, attr, True)


def _get_service_connection(resource_arn):
    """
    Returns a connection to an AWS Service. Uses a local cache to help
    out with performance.

    :param resource_arn: an AWS resource ARN like
      'arn:partition:kinesis:region:account:resource'
    :return: a boto3 connection
    """
    arn = get_arn_from_arn_string(resource_arn)
    with _lock:

        # the local var is of the form "_kinesis_eu-west-1_connection"
        attr = arn.service + '_' + arn.region_name + '_connection'
        if not getattr(_local, attr, None):

            # determine the actual region_name and endpoint url if we are
            # running the services locally.
            service, region_name, endpoint_url = _get_connection_info(arn.service, arn.region_name)

            logger.debug("Initializing connection for service: %s, region_name: %s, endpoint_url: %s",
                         service, region_name, endpoint_url)

            # for elasticache/memcache, we need to ensure that an actual endpoint
            # is specified, since the memcache library doesn't have all the default
            # logic used in the boto3 library
            if service == AWS.ELASTICACHE:
                if not endpoint_url:
                    return None
                endpoint_url = endpoint_url.split(',')
                import memcache
                connection = memcache.Client(endpoint_url)

            # actual AWS services with boto3 APIs
            else:
                connection = \
                    boto3.client(service, region_name=region_name, endpoint_url=endpoint_url)

            # wrapped in a chaos connection if applicable
            if getattr(settings, 'AWS_CHAOS', {}):
                connection = ChaosConnection(service, connection)  # pragma: no cover

            setattr(_local, attr, connection)
        return getattr(_local, attr)


def get_connection(resource_arn):
    """
    Returns a connection to an appropriate service ARN. Since the ARN
    has the region and service encoded, it is possible to figure out all
    the appropriate connection settings.

    :param resource_arn: an AWS resource ARN like
      'arn:partition:kinesis:region:account:resource'
    :return: a boto3 connection
    """
    connection = None
    service = get_arn_from_arn_string(resource_arn).service
    # possibly short circuit if settings.USE_FOO is False
    if _service_enabled(service):
        connection = _get_service_connection(resource_arn)
    # for now, always return _something_ for the cache layer
    # even if that something is to do nothing.
    if service == AWS.ELASTICACHE:
        connection = connection or LOCAL_CACHE
    return connection


class NoOpCache(object):  # pragma: no cover
    """
    A cache object with an interface like memcache.Client, but
    doesn't actually cache anything.
    """
    def add(self, key, value):
        return True

    def get(self, key):
        return None

    def set(self, key, value):
        return True

    def delete(self, key):
        return True


LOCAL_CACHE = NoOpCache()


def _trace(func, *args, **kwargs):
    """
    Logs a TRACE level message.

    :param func: the callable function to call
    :param args: the args for the function
    :param kwargs: the kwargs for the function
    :return: the return value of the function
    """
    guid = uuid.uuid4().hex
    logger.log(TRACE, '%s: function=%s, args=%s, kwargs=%s)', guid, func, args, kwargs)
    return_value = func(*args, **kwargs)
    logger.log(TRACE, '%s: return_value = %s', guid, return_value)
    return return_value


def get_primary_cache_source():
    return settings.PRIMARY_CACHE_SOURCE


def get_primary_stream_source():
    return os.environ.get(ENVIRONMENT.FSM_PRIMARY_STREAM_SOURCE) or \
           settings.PRIMARY_STREAM_SOURCE


def get_secondary_stream_source():
    return os.environ.get(ENVIRONMENT.FSM_SECONDARY_STREAM_SOURCE) or \
           settings.SECONDARY_STREAM_SOURCE


def get_primary_checkpoint_source():
    return settings.PRIMARY_CHECKPOINT_SOURCE


def get_secondary_checkpoint_source():
    return settings.SECONDARY_CHECKPOINT_SOURCE


def get_primary_retry_source():
    return settings.PRIMARY_RETRY_SOURCE


def get_secondary_retry_source():
    return settings.SECONDARY_RETRY_SOURCE


def get_primary_environment_source():
    return settings.PRIMARY_ENVIRONMENT_SOURCE


def get_secondary_environment_source():
    return settings.SECONDARY_ENVIRONMENT_SOURCE


def get_primary_metrics_source():
    return settings.PRIMARY_METRICS_SOURCE


def increment_error_counters(data, dimensions):
    """
    Increments an error counter in AWS CloudWatch.

    :return: the response from boto3 put_metric_data call.
    """
    source = get_primary_metrics_source()
    cloudwatch_conn = get_connection(source)
    if not cloudwatch_conn:
        return

    namespace = get_arn_from_arn_string(source).resource
    utcnow = datetime.datetime.utcnow()
    return_value = _trace(
        cloudwatch_conn.put_metric_data,
        Namespace=namespace,
        MetricData=[
            {
                AWS_CLOUDWATCH.MetricName: name,
                AWS_CLOUDWATCH.Dimensions: [
                    {AWS_CLOUDWATCH.Name: key, AWS_CLOUDWATCH.Value: val} for key, val in dimensions.iteritems()
                    ],
                AWS_CLOUDWATCH.Timestamp: utcnow,
                AWS_CLOUDWATCH.Value: value
            } for name, value in data.items()
            ]
    )
    return return_value


def _set_message_dispatched_memcache(cache_arn, correlation_id, steps):
    """Sets a flag in memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    key = '%s-%s' % (correlation_id, steps)
    return_value = memcache_conn.set(key, True)
    return return_value


def _set_message_dispatched_dynamodb(table_arn, correlation_id, steps):
    """Sets a flag in dynamodb"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    cache_key = '%s-%s' % (correlation_id, steps)
    item = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
        CACHE_DATA.VALUE: {AWS_DYNAMODB.BOOLEAN: True}
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    return_value = _trace(
        dynamodb_conn.put_item,
        TableName=table_name,
        Item=item
    )
    return bool(return_value)


def set_message_dispatched(correlation_id, steps, primary=True):
    """
    Sets a flag in cache to indicate that a message has been dispatched.
    This is used by the framework to ensure that actions are not executed
    multiple times if the messages are received multiple times.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :return: True if cached and False otherwise
    """
    source_arn = get_primary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No cache source for primary=%s" % primary)

    elif service == AWS.CLOUDWATCH:
        return _set_message_dispatched_memcache(source_arn, correlation_id, steps)

    elif service == AWS.DYNAMODB:
        return _set_message_dispatched_dynamodb(source_arn, correlation_id, steps)


def _get_message_dispatched_memcache(cache_arn, correlation_id, steps):
    """Gets a flag from memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return False  # pragma: no cover

    key = '%s-%s' % (correlation_id, steps)
    return_value = memcache_conn.get(key)
    return return_value


def _get_message_dispatched_dynamodb(table_arn, correlation_id, steps):
    """Gets a flag from dynamodb"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    cache_key = '%s-%s' % (correlation_id, steps)
    key = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    return_value = _trace(
        dynamodb_conn.get_item,
        ConsistentRead=True,
        TableName=table_name,
        Key=key
    )
    return return_value.get(AWS_DYNAMODB.Item, {}) \
                       .get(CACHE_DATA.VALUE, {}) \
                       .get(AWS_DYNAMODB.BOOLEAN, False)


def get_message_dispatched(correlation_id, steps, primary=True):
    """
    Sets a flag in cache to indicate that a message has been dispatched.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :return: True if cached and False otherwise
    """
    source_arn = get_primary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No cache source for primary=%s" % primary)

    elif service == AWS.CLOUDWATCH:
        return _get_message_dispatched_memcache(source_arn, correlation_id, steps)

    elif service == AWS.DYNAMODB:
        return _get_message_dispatched_dynamodb(source_arn, correlation_id, steps)


def _cache_add_memcache(cache_arn, key, value):
    """Adds a value to memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    return_value = memcache_conn.add(key, value)
    return return_value


def _cache_add_dynamodb(table_arn, key, value):
    """Adds a value to DynamoDB cache"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    item = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: key},
        CACHE_DATA.VALUE: {AWS_DYNAMODB.STRING: value}
    }

    # add the value to memcache
    try:
        return_value = _trace(
            dynamodb_conn.put_item,
            TableName=table_name,
            Item=item,
            ConditionExpression='attribute_not_exists(%s)' % CACHE_DATA.KEY
        )
        return bool(return_value)

    except ClientError, e:

        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise  # pragma: no cover
        return False


def cache_add(key, value, primary=True):
    """
    Adds a value to cache. Returns True if it was added, and False otherwise.

    :param key: a str cache key
    :param value: a str cache value
    :return: True if added and False otherwise
    """

    source_arn = get_primary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No cache source for primary=%s" % primary)

    elif service == AWS.CLOUDWATCH:
        return _cache_add_memcache(source_arn, key, value)

    elif service == AWS.DYNAMODB:
        return _cache_add_dynamodb(source_arn, key, value)


def _cache_get_memcache(cache_arn, cache_key):
    """Gets a value from memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    return_value = memcache_conn.get(cache_key)
    return return_value


def _cache_get_dynamodb(table_arn, cache_key):
    """Gets a value from DynamoDB cache"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    key = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
    }

    # get the value from cache
    return_value = _trace(
        dynamodb_conn.get_item,
        ConsistentRead=True,
        TableName=table_name,
        Key=key
    )
    return return_value.get(AWS_DYNAMODB.Item, {}) \
                       .get(CACHE_DATA.VALUE, {}) \
                       .get(AWS_DYNAMODB.STRING, None)


def cache_get(key, primary=True):
    """
    Gets a value from cache.

    :param key: a str cache key
    :return: the str value in cache
    """

    source_arn = get_primary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No cache source for primary=%s" % primary)

    elif service == AWS.CLOUDWATCH:
        return _cache_get_memcache(source_arn, key)

    elif service == AWS.DYNAMODB:
        return _cache_get_dynamodb(source_arn, key)


def _cache_delete_memcache(cache_arn, cache_key):
    """Deletes a value from memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    return_value = memcache_conn.delete(cache_key)
    return return_value


def _cache_delete_dynamodb(table_arn, cache_key):
    """Deletes a value from DynamoDB cache"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    key = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
    }

    # delete the value from cache
    return_value = _trace(
        dynamodb_conn.delete_item,
        TableName=table_name,
        Key=key
    )
    return bool(return_value)


def cache_delete(key, primary=True):
    """
    Gets a value from cache.

    :param key: a str cache key
    :return: the str value in cache
    """

    source_arn = get_primary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No cache source for primary=%s" % primary)

    elif service == AWS.CLOUDWATCH:
        return _cache_delete_memcache(source_arn, key)

    elif service == AWS.DYNAMODB:
        return _cache_delete_dynamodb(source_arn, key)


def _send_next_event_for_dispatch_kinesis(stream_arn, data, correlation_id):
    """
    Sends an FSM event message onto Kinesis.

    :param stream_arn: a str ARN for a kinesis stream like
      'arn:partition:kinesis:region:account:resource'
    :param data: a str data for the kinesis message
    :param correlation_id: the guid for the fsm
    :return: the return value from boto3 put_record call
    """
    # write the event and fsm state to kinesis.
    kinesis_conn = get_connection(stream_arn)
    if not kinesis_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(stream_arn).resource
    stream_name = resource.split('/')[-1]
    return_value = _trace(
        kinesis_conn.put_record,
        StreamName=stream_name,
        Data=data,
        PartitionKey=correlation_id
    )
    return return_value


def _send_next_event_for_dispatch_dynamodb(table_arn, data, correlation_id):
    """
    Sends an FSM event message onto DyanmoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param data: a str data for the dynamodb message
    :param correlation_id: the guid for the fsm
    :return: the return value from boto3 put_item call
    """
    # write the event and fsm state to dynamodb.
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    timestamp = str(int(time.time()))
    item = {
        STREAM_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id},
        STREAM_DATA.PAYLOAD: {AWS_DYNAMODB.STRING: data},
        STREAM_DATA.TIMESTAMP: {AWS_DYNAMODB.NUMBER: timestamp}
    }
    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    return_value = _trace(
        dynamodb_conn.put_item,
        TableName=table_name,
        Item=item
    )
    return return_value


def _send_next_event_for_dispatch_sns(topic_arn, data, correlation_id):
    """
    Sends an FSM event message onto SNS.

    :param topic_arn: a str ARN for a SNS topic like
      'arn:partition:sns:region:account:resource'
    :param data: a str data for the sns message
    :param correlation_id: the guid for the fsm
    :return: the return value from boto3 publish call
    """
    # write the event and fsm state to sns.
    sns_conn = get_connection(topic_arn)
    if not sns_conn:
        return  # pragma: no cover

    return_value = _trace(
        sns_conn.publish,
        TopicArn=topic_arn,
        Message=json.dumps({"default": data})
    )
    return return_value


def _send_next_event_for_dispatch_sqs(queue_arn, data, correlation_id, delay):
    """
    Sends an FSM event message onto SQS.

    :param queue_arn: a str ARN for a SQS queue like
      'arn:partition:sqs:region:account:resource'
    :param data: a str data for the sns message
    :param correlation_id: the guid for the fsm
    :return: the return value from boto3 publish call
    """
    # write the event and fsm state to sqs.
    sqs_conn = get_connection(queue_arn)
    if not sqs_conn:
        return  # pragma: no cover

    arn = get_arn_from_arn_string(queue_arn)
    queue_url = AWS_SQS.URI_TEMPLATE % vars(arn)
    return_value = _trace(
        sqs_conn.send_message,
        QueueUrl=queue_url,
        MessageBody=data,
        DelaySeconds=delay
    )
    return return_value


def send_next_event_for_dispatch(context, data, correlation_id, delay=0, primary=True):
    """
    Sends an FSM event message onto Kinesis or DynamoDB or SNS.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param data: a str data for the sns message
    :param correlation_id: the guid for the fsm
    :param primary: if True, use the primary stream source, and if False
      use the secondary stream source
    :return: see above.
    """
    if primary:
        source_arn = get_primary_stream_source()
    else:
        source_arn = get_secondary_stream_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No stream source for primary=%s" % primary)

    elif service == AWS.KINESIS:
        return _send_next_event_for_dispatch_kinesis(source_arn, data, correlation_id)

    elif service == AWS.DYNAMODB:
        return _send_next_event_for_dispatch_dynamodb(source_arn, data, correlation_id)

    elif service == AWS.SNS:
        return _send_next_event_for_dispatch_sns(source_arn, data, correlation_id)

    elif service == AWS.SQS:
        return _send_next_event_for_dispatch_sqs(source_arn, data, correlation_id, delay)


def _send_next_events_for_dispatch_kinesis(stream_arn, all_data, correlation_ids):
    """
    Sends multiple FSM event message onto Kinesis.

    :param table_arn: a str ARN for a Kinesis table like
      'arn:partition:kinesis:region:account:resource'
    :param all_data: a list of str data for the message
    :param all_correlation_ids: a list of guids for the fsms
    :return: a return value from boto3 put_records call
    """
    # write the event and fsm state to kinesis.
    kinesis_conn = get_connection(stream_arn)
    if not kinesis_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(stream_arn).resource
    stream_name = resource.split('/')[-1]
    return_value = _trace(
        kinesis_conn.put_records,
        StreamName=stream_name,
        Records=[
            {
                AWS_KINESIS.RECORD.Data: data,
                AWS_KINESIS.RECORD.PartitionKey: correlation_id
            }
            for (data, correlation_id) in zip(all_data, correlation_ids)
        ]
    )
    return return_value


def _send_next_events_for_dispatch_dynamodb(table_arn, all_data, correlation_ids):
    """
    Sends multiple FSM event message onto DyanomoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param all_data: a list of str data for the message
    :param all_correlation_ids: a list of guids for the fsms
    :return: a return value from boto3 batch_write_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    request_items = {table_name: []}
    timestamp = str(int(time.time()))
    for data, correlation_id in zip(all_data, correlation_ids):
        item = {
            AWS_DYNAMODB.PutRequest: {
                AWS_DYNAMODB.Item: {
                    STREAM_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id},
                    STREAM_DATA.PAYLOAD: {AWS_DYNAMODB.STRING: data},
                    STREAM_DATA.TIMESTAMP: {AWS_DYNAMODB.NUMBER: timestamp}
                }
            }
        }
        request_items[table_name].append(item)
    return_value = _trace(
        dynamodb_conn.batch_write_item,
        RequestItems=request_items
    )
    return return_value


def _send_next_events_for_dispatch_sns(topic_arn, all_data, correlation_ids):
    """
    Sends multiple FSM event message onto SNS.

    :param topic_arn: a str ARN for a SNS topic like
      'arn:partition:sns:region:account:resource'
    :param all_data: a list of str data for the message
    :param all_correlation_ids: a list of guids for the fsms
    :return: a list of return values from boto3 publish call
    """
    # write the event and fsm state to sns.
    return_value = []
    for (correlation_id, data) in zip(correlation_ids, all_data):
        ret = _send_next_event_for_dispatch_sns(topic_arn, data, correlation_id)  # no bulk endpoint
        return_value.append(ret)
    return return_value


def _send_next_events_for_dispatch_sqs(queue_arn, all_data, correlation_ids, delay):
    """
    Sends multiple FSM event message onto SQS.

    :param queue_arn: a str ARN for a SQS queue like
      'arn:partition:sqs:region:account:resource'
    :param all_data: a list of str data for the message
    :param all_correlation_ids: a list of guids for the fsms
    :return: a list of return values from boto3 publish call
    """
    # write the event and fsm state to sqs.
    sqs_conn = get_connection(queue_arn)
    if not sqs_conn:
        return  # pragma: no cover

    arn = get_arn_from_arn_string(queue_arn)
    queue_url = AWS_SQS.URI_TEMPLATE % vars(arn)
    entries = [
        {
            AWS_SQS.MESSAGE.Id: correlation_id,
            AWS_SQS.MESSAGE.MessageBody: data,
            AWS_SQS.MESSAGE.DelaySeconds: delay
        }
        for data, correlation_id in zip(all_data, correlation_ids)
    ]
    return_value = _trace(
        sqs_conn.send_message_batch,
        QueueUrl=queue_url,
        Entries=entries
    )
    return return_value


def send_next_events_for_dispatch(context, all_data, correlation_ids, delay=0, primary=True):
    """
    Sends multiple FSM event message onto Kinesis or DynamoDB or SNS.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param all_data: a list of str data for the message
    :param correlation_ids: a list of guids for the fsms
    :param primary: if True, use the primary stream source, and if False
      use the secondary stream source
    :return: see above.
    """
    if primary:
        source_arn = get_primary_stream_source()
    else:
        source_arn = get_secondary_stream_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No stream source for primary=%s" % primary)

    if service == AWS.KINESIS:
        return _send_next_events_for_dispatch_kinesis(source_arn, all_data, correlation_ids)

    elif service == AWS.DYNAMODB:
        return _send_next_events_for_dispatch_dynamodb(source_arn, all_data, correlation_ids)

    elif service == AWS.SNS:
        return _send_next_events_for_dispatch_sns(source_arn, all_data, correlation_ids)

    elif service == AWS.SQS:
        return _send_next_events_for_dispatch_sqs(source_arn, all_data, correlation_ids, delay)


def _store_checkpoint_dynamodb(table_arn, correlation_id, sent):
    """
    Stores the return value from a prior call to send_next_event_for_dispatch to
    DyanamoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param sent: the data to checkpoint
    :return: the return value from boto3 put_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    partition = int(hashlib.md5(correlation_id).hexdigest(), 16) % 16
    item = {
        RECOVERY_DATA.PARTITION: {AWS_DYNAMODB.NUMBER: str(partition)},
        RECOVERY_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id},
        RECOVERY_DATA.SENT: {AWS_DYNAMODB.STRING: sent}
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    return_value = _trace(
        dynamodb_conn.put_item,
        TableName=table_name,
        Item=item
    )
    return return_value


def store_checkpoint(context, sent, primary=True):
    """
    Stores the return value from a prior call to send_next_event_for_dispatch to
    persistent storage so that a stalled FSM can be re-started from the last known
    prior state.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param sent: the data to checkpoint
    :param primary: if True, use the primary checkpoint source, and if False
      use the secondary checkpoint source
    :return: see above.
    """
    if primary:
        source_arn = get_primary_checkpoint_source()
    else:
        source_arn = get_secondary_checkpoint_source()

    service = get_arn_from_arn_string(source_arn).service

    if service == AWS.DYNAMODB:
        return _store_checkpoint_dynamodb(source_arn, context.correlation_id, sent)


def _store_environment_dynamodb(table_arn, environment):
    """
    Stores an environment dict into DynamoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param environment: a dict of {str: str}
    :return: a tuple of a guid and the return value from boto3 put_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return None, None  # pragma: no cover

    serialized = json.dumps(environment)
    guid = uuid.uuid4().hex
    item = {
        ENVIRONMENT_DATA.GUID: {AWS_DYNAMODB.STRING: guid},
        ENVIRONMENT_DATA.ENVIRONMENT: {AWS_DYNAMODB.STRING: serialized},
    }

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]

    # write the environment offset to dynamodb. this allows us lookup LARGE
    # envonments and get around 8192 character limits in ECS
    return_value = _trace(
        dynamodb_conn.put_item,
        TableName=table_name,
        Item=item
    )
    return guid, return_value


def store_environment(context, environment, primary=True):
    """
    Stores an environment dict into persistent storage. This helps get
    around the 8192 character limit in ECS Tasks.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param environment: a dict of {str: str}
    :param primary: if True, use the primary environment source, and if False
      use the secondary environment source
    :return: see above.
    """

    if primary:
        source_arn = get_primary_environment_source()
    else:
        source_arn = get_secondary_environment_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No environment source for primary=%s" % primary)

    if service == AWS.DYNAMODB:
        guid, return_value = _store_environment_dynamodb(source_arn, environment)
        if guid:
            return source_arn + ';' + guid, return_value


def _load_environment_dynamodb(table_arn, guid):
    """
    Loads an environment dict from DynamoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param key: a guid key of the dynamodb entity
    :return: the environment dict of {str: str}
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    key = {
        ENVIRONMENT_DATA.GUID: {AWS_DYNAMODB.STRING: guid}
    }

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]

    # load the environment from dynamodb
    item = _trace(
        dynamodb_conn.get_item,
        ConsistentRead=True,
        TableName=table_name,
        Key=key
    )

    if item:
        serialized = item[AWS_DYNAMODB.Item][ENVIRONMENT_DATA.ENVIRONMENT][AWS_DYNAMODB.STRING]
        environment = json.loads(serialized)
        return environment


def load_environment(context, key, primary=True):
    """
    Loads an environment dict from persistent storage. This helps get
    around the 8192 character limit in ECS Tasks.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param key: a str key as returned from store_environment
    :param primary: if True, use the primary environment source, and if False
      use the secondary environment source
    :return: see above.
    """

    source, guid = key.split(';')
    service = get_arn_from_arn_string(source).service

    if not service:  # pragma: no cover
        logger.warning("No environment source for primary=%s" % primary)

    if service == AWS.DYNAMODB:
        return _load_environment_dynamodb(source, guid)


def _start_retries_dynamodb(table_arn, correlation_id, run_at, payload):
    """
    Triggers retries for a state machine by sending a message to DynamoDB.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param run_at: a integer time since epoch
    :param payload: the retry payload (serialized fsm context)
    :return: the return value from boto3 put_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    partition = int(hashlib.md5(correlation_id).hexdigest(), 16) % 16
    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    item = {
        RECOVERY_DATA.PARTITION: {AWS_DYNAMODB.NUMBER: str(partition)},
        RECOVERY_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id},
        RECOVERY_DATA.RUN_AT: {AWS_DYNAMODB.NUMBER: str(run_at)},
        RECOVERY_DATA.PAYLOAD: {AWS_DYNAMODB.STRING: payload}
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    return_value = _trace(
        dynamodb_conn.put_item,
        TableName=table_name,
        Item=item
    )
    return return_value


def _start_retries_kinesis(stream_arn, correlation_id, payload):
    """
    Triggers retries for a state machine by sending a message to Kinesis.

    NOTE: Backoff is not supported.

    :param stream_arn: a str ARN for a Kinesis stream like
      'arn:partition:kinesis:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param payload: the retry payload (serialized fsm context)
    :return: the return value from _send_next_event_for_dispatch_kinesis
    """
    return_value = _send_next_event_for_dispatch_kinesis(stream_arn, payload, correlation_id)
    return return_value


def _start_retries_sns(topic_arn, correlation_id, payload):
    """
    Triggers retries for a state machine by sending a message to SNS.

    NOTE: Backoff is not supported.

    :param topic_arn: a str ARN for a SNS topic like
      'arn:partition:sns:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param run_at: a integer time since epoch
    :param payload: the retry payload (serialized fsm context)
    :return: the return value from _send_next_event_for_dispatch_sns
    """
    return_value = _send_next_event_for_dispatch_sns(topic_arn, payload, correlation_id)
    return return_value


def _start_retries_sqs(queue_arn, correlation_id, run_at, payload):
    """
    Triggers retries for a state machine by sending a message to SQS.

    :param queue_arn: a str ARN for a SQS queue like
      'arn:partition:sqs:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param run_at: a integer time since epoch
    :param payload: the retry payload (serialized fsm context)
    :return: the return value from boto3 send_message call
    """
    sqs_conn = get_connection(queue_arn)
    if not sqs_conn:
        return  # pragma: no cover

    # write the event and fsm state to sqs.
    sqs_conn = get_connection(queue_arn)
    arn = get_arn_from_arn_string(queue_arn)
    queue_url = AWS_SQS.URI_TEMPLATE % vars(arn)
    now = int(time.time())
    run_at_minus_now = max(0, run_at - now)  # might be negative
    delay_seconds = min(AWS_SQS.MAX_DELAY_SECONDS, run_at_minus_now)
    delay_seconds = int(delay_seconds)
    return_value = _trace(
        sqs_conn.send_message,
        QueueUrl=queue_url,
        MessageBody=payload,
        DelaySeconds=delay_seconds
    )
    return return_value


def start_retries(context, run_at, payload, primary=True):
    """
    Triggers retries for a state machine by sending a message to a "run_at"
    parameter designating when to run the retry.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param run_at: a integer time since epoch
    :param payload: the retry payload (serialized fsm context)
    :param primary: if True, use the primary retries source, and if False
      use the retries environment source
    :return: see above.
    """
    if primary:
        source_arn = get_primary_retry_source()
    else:
        source_arn = get_secondary_retry_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No retry source for primary=%s" % primary)

    elif service == AWS.KINESIS:
        return _start_retries_kinesis(source_arn, context.correlation_id, payload)

    elif service == AWS.DYNAMODB:
        return _start_retries_dynamodb(source_arn, context.correlation_id, run_at, payload)

    elif service == AWS.SNS:
        return _start_retries_sns(source_arn, context.correlation_id, payload)

    elif service == AWS.SQS:
        return _start_retries_sqs(source_arn, context.correlation_id, run_at, payload)


def _stop_retries_dynamodb(table_arn, correlation_id):
    """
    Stops retries for a state machine by deleting any persistent messages
    that trigger retires.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param correlation_id: the guid for the fsm
    :return: a return value from boto3 delete_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    partition = int(hashlib.md5(correlation_id).hexdigest(), 16) % 16
    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]
    key = {
        RECOVERY_DATA.PARTITION: {AWS_DYNAMODB.NUMBER: str(partition)},
        RECOVERY_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id}
    }

    # delete a dynamodb entity
    return_value = _trace(
        dynamodb_conn.delete_item,
        TableName=table_name,
        Key=key
    )
    return return_value


def stop_retries(context, primary=True):
    """
    Stops retries for a state machine by deleting any persistent messages
    that trigger retires.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param primary: if True, use the primary retries source, and if False
      use the retries environment source
    :return: see above.
    """
    if primary:
        source_arn = get_primary_retry_source()
    else:
        source_arn = get_secondary_retry_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        logger.warning("No retry source for primary=%s" % primary)

    elif service == AWS.DYNAMODB:
        return _stop_retries_dynamodb(source_arn, context.correlation_id)


def retriable_entities(table_arn, index, run_at, limit=100):
    """

    :param table:
    :param index:
    :param run_at:
    :param limit:
    :return:
    """
    # query for some dynamodb entities
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return []

    items = []

    resource = get_arn_from_arn_string(table_arn).resource
    table_name = resource.split('/')[-1]

    for partition in xrange(16):

        # query by partition
        results = _trace(
            dynamodb_conn.query,
            TableName=table_name,
            ConsistentRead=True,
            IndexName=index,
            KeyConditions={
                RECOVERY_DATA.PARTITION: {
                    AWS_DYNAMODB.ComparisonOperator: AWS_DYNAMODB.EQUAL,
                    AWS_DYNAMODB.AttributeValueList: [{AWS_DYNAMODB.NUMBER: str(partition)}]
                },
                RECOVERY_DATA.RUN_AT: {
                    AWS_DYNAMODB.ComparisonOperator: AWS_DYNAMODB.LESS_THAN,
                    AWS_DYNAMODB.AttributeValueList: [{AWS_DYNAMODB.NUMBER: str(run_at)}]
                }
            },
            Limit=limit
        )[AWS_DYNAMODB.Items]

        for result in results:
            # pull the payload out of the item
            items.append(
                {
                    RECOVERY_DATA.PAYLOAD: result[RECOVERY_DATA.PAYLOAD][AWS_DYNAMODB.STRING],
                    RECOVERY_DATA.CORRELATION_ID: result[RECOVERY_DATA.CORRELATION_ID][AWS_DYNAMODB.STRING],
                }
            )

    return items
