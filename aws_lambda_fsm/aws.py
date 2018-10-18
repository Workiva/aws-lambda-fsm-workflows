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
from botocore.endpoint import DEFAULT_TIMEOUT
from botocore.exceptions import ClientError
from botocore.client import Config

# application imports
from aws_lambda_fsm.constants import ENVIRONMENT_DATA
from aws_lambda_fsm.constants import RETRY_DATA
from aws_lambda_fsm.constants import CHECKPOINT_DATA
from aws_lambda_fsm.constants import CACHE_DATA
from aws_lambda_fsm.constants import LEASE_DATA
from aws_lambda_fsm.constants import STREAM_DATA
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.constants import AWS_KINESIS
from aws_lambda_fsm.constants import AWS_CLOUDWATCH
from aws_lambda_fsm.constants import AWS_ELASTICACHE
from aws_lambda_fsm.constants import AWS_SQS
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import ENVIRONMENT
from aws_lambda_fsm.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class Object(object):
    pass


_local = Object()
_loglock = RLock()
_rlock = RLock()


TRACE = 5
ALREADY_LOGGED = set()
ALREADY_LOGGED_MAX_SIZE = 100


def log_once(method, *args, **kwargs):
    """
    Convenience message to ensure a given log message is only emitted once.

    :param method: a method like logger.debug, logger.warn
    :param args: a list of args for the logger method
    :param kwargs: a dict of kwargs for the logger method
    """
    key = "%s-%s-%s" % (getattr(method, '__name__', method), args, kwargs)
    with _loglock:
        if key not in ALREADY_LOGGED:
            method(*args, **kwargs)
            # do not add messages without bound
            if len(ALREADY_LOGGED) < ALREADY_LOGGED_MAX_SIZE:
                ALREADY_LOGGED.add(key)


class ChaosFunction(object):
    """
    A callable class that raises an exception or returns a fixed (error) value.
    Used by ChaosConnection when a error is to be returned.
    """

    def __init__(self, exception_or_return, wrapped_function):
        self.exception_or_return = exception_or_return
        self.wrapped_function = wrapped_function

    def __call__(self, *args, **kwargs):
        # 50% of the time, actually call the function
        if random.uniform(0.0, 1.0) < 0.5:
            self.wrapped_function(*args, **kwargs)
        if isinstance(self.exception_or_return, Exception):
            raise self.exception_or_return
        else:
            return self.exception_or_return


class ChaosConnection(object):
    """
    A wrapper for a memcache.Client or a botocore.Client that raises an exception
    or returns a fixed (error) value.
    """

    def __init__(self, resource_arn, connection, chaos=None):
        chaos = chaos or getattr(settings, 'AWS_CHAOS', {})
        self.original_chaos = chaos
        self.resource_arn = resource_arn
        self.wrapped_connection = connection
        self.chaos = chaos.get(resource_arn, {})
        if os.environ.get('DISABLE_AWS_CHAOS'):
            self.chaos = {}  # pragma: no cover

    def __getattr__(self, attr):
        original_attr = getattr(self.wrapped_connection, attr)
        if self.chaos:
            if attr == 'pipeline':
                return ChaosConnection(self.resource_arn, original_attr, self.original_chaos)
            if callable(original_attr):
                for exception_or_return, percentage in self.chaos.iteritems():
                    if random.uniform(0.0, 1.0) < percentage:
                        return ChaosFunction(exception_or_return, original_attr)
        return original_attr

    def __call__(self, *args, **kwargs):
        return_value = self.wrapped_connection(*args, **kwargs)
        if return_value is not None:
            return_value = ChaosConnection(self.resource_arn, return_value, self.original_chaos)
        return return_value

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wrapped_connection.__exit__(exc_type, exc_val, exc_tb)

    def __enter__(self):
        self.wrapped_connection.__enter__()
        return self


class Arn(namedtuple('Arn', ['arn', 'partition', 'service', 'region_name', 'account_id', 'resource'])):

    __slots__ = ()

    def slash_resource(self):
        if not self.resource:
            return None
        return self.resource.split('/')[-1]

    def slash_resource_type(self):
        if not self.resource:
            return None
        return self.resource.split('/')[0]

    def colon_resource(self):
        if not self.resource:
            return None
        return self.resource.split(':')[-1]

    def colon_resource_type(self):
        if not self.resource:
            return None
        return self.resource.split(':')[0]


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
            parts += [None] * (6 - len(parts))
        return Arn(*parts)
    else:
        return Arn(None, None, None, None, None, None)


def _get_elasticache_engine_and_connection(cache_arn):
    """
    Returns a cache client suitable to for the specified resource ARN.

    :param cache_arn: an Elasticache resource ARN
    :return: either a redis.StrictRedis or memcache.Client object
    """
    arn = get_arn_from_arn_string(cache_arn)

    # first lookup via settings.ENDPOINTS by service (backwards compatability)
    #
    # ENDPOINTS = {
    #   'elasticache': {
    #     'testing': 'host:9999'
    #   }
    # }
    #
    if AWS.ELASTICACHE in getattr(settings, 'ENDPOINTS', {}):
        log_once(logger.warning, 'settings.ENDPOINTS is deprecated for Elasticache.')
        hostport = settings.ENDPOINTS[AWS.ELASTICACHE].get(arn.region_name)
        cfg = {
            AWS_ELASTICACHE.CacheClusterId: 'unused',
            AWS_ELASTICACHE.ConfigurationEndpoint: {
                AWS_ELASTICACHE.ENDPOINT.Address: hostport.split(':')[0],
                AWS_ELASTICACHE.ENDPOINT.Port: int(hostport.split(':')[1])
            }
        }
        return AWS_ELASTICACHE.ENGINE.MEMCACHED, _get_memcached_connection(cache_arn, cfg)

    # next lookup via settings.ENDPOINTS by arn (backwards compatability)
    #
    # ENDPOINTS = {
    #   'cache_arn': 'host:9999'
    # }
    #
    if cache_arn in getattr(settings, 'ENDPOINTS', {}):
        log_once(logger.warning, 'settings.ENDPOINTS is deprecated for Elasticache.')
        hostport = settings.ENDPOINTS[cache_arn]
        cfg = {
            AWS_ELASTICACHE.CacheClusterId: 'unused',
            AWS_ELASTICACHE.ConfigurationEndpoint: {
                AWS_ELASTICACHE.ENDPOINT.Address: hostport.split(':')[0],
                AWS_ELASTICACHE.ENDPOINT.Port: int(hostport.split(':')[1])
            }
        }
        return AWS_ELASTICACHE.ENGINE.MEMCACHED, _get_memcached_connection(cache_arn, cfg)

    # next lookup via settings.ELASTICACHE_ENDPOINTS data
    #
    # ELASTICACHE_ENDPOINTS = {
    #
    #   'memcached_cache_cluster_arn': {
    #     'CacheClusterId': 'abc123',
    #     'TransitEncryptionEnabled': False,
    #     'AuthTokenEnabled': False,
    #     'Engine': 'memcached',
    #     'ConfigurationEndpoint': {
    #       'Address': 'host',
    #       'Port': 9999
    #     },  ...
    #   },
    #
    #   'redis_cache_cluster_arn': {
    #     'CacheClusterId': 'def456',
    #     'TransitEncryptionEnabled': True,
    #     'AuthTokenEnabled': True,
    #     'Engine': 'redis',
    #     'CacheNodes': [
    #       {
    #         'Endpoint': {
    #           'Address': 'host',
    #           'Port': 9999
    #          }, ...
    #       }, ...
    #     }, ...
    #   },
    #
    #   'redis_replication_group_arn': {
    #     'ReplicationGroupId': 'ghi789',
    #     'TransitEncryptionEnabled': True,
    #     'AuthTokenEnabled': True,
    #     'NodeGroups': [
    #       {
    #         'PrimaryEndpoint': {
    #           'Address': 'host',
    #           'Port': 9999
    #         }, ...
    #       }
    #     ]
    #   }
    #
    if cache_arn in getattr(settings, 'ELASTICACHE_ENDPOINTS', {}):
        entry = settings.ELASTICACHE_ENDPOINTS.get(cache_arn, {})

        # cache clusters
        if AWS_ELASTICACHE.CacheClusterId in entry:
            engine = entry.get(AWS_ELASTICACHE.Engine)
            if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
                return AWS_ELASTICACHE.ENGINE.MEMCACHED, _get_memcached_connection(cache_arn, entry)
            elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
                return AWS_ELASTICACHE.ENGINE.REDIS, _get_redis_connection(cache_arn, entry)

        # replication groups
        elif AWS_ELASTICACHE.ReplicationGroupId in entry:
            return AWS_ELASTICACHE.ENGINE.REDIS, _get_redis_connection(cache_arn, entry)

    # since this makes an external call, which may be expensive, we don't bother
    # locking like in other locations where _local is mutated. the url never changes
    # so looking it up in a couple threads simultaneously does not harm. also
    # aws lambda dispatch model doesn't appear to use multiple threads anyway.

    attr = 'cache_details_for_' + cache_arn
    if not getattr(_local, attr, None):

        log_once(logger.warning, 'Consider using settings.ELASTICACHE_ENDPOINTS for endpoints.')

        elasticache_connection = boto3.client('elasticache', region_name=arn.region_name)

        engine = connection = None

        try:
            # first look in cache clusters
            return_value = _trace(
                elasticache_connection.describe_cache_clusters,
                CacheClusterId=arn.colon_resource(),
                ShowCacheNodeInfo=True
            )

            cluster = return_value[AWS_ELASTICACHE.CacheClusters][0]
            engine = cluster[AWS_ELASTICACHE.Engine]
            if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
                connection = _get_memcached_connection(cache_arn, cluster)
            elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
                connection = _get_redis_connection(cache_arn, cluster)

        except ClientError:
            engine = connection = None

        if not engine or not connection:

            try:

                # otherwise look in the replication groups
                return_value = _trace(
                    elasticache_connection.describe_replication_groups,
                    ReplicationGroupId=arn.colon_resource()
                )

                engine = AWS_ELASTICACHE.ENGINE.REDIS
                group = return_value[AWS_ELASTICACHE.ReplicationGroups][0]
                connection = _get_redis_connection(cache_arn, group)

            except ClientError:
                engine = connection = None

        # not found in any of the usual places
        if not engine or not connection:
            log_once(logger.fatal, "Cache ARN %s is not valid.", cache_arn)

        setattr(_local, attr, (engine, connection))

    return getattr(_local, attr)


def get_connection_info(service, region_name, resource_arn):
    """
    Returns the service, region_name and endpoint_url to use when creating
    a boto3 connection. In settings.ENDPOINTS it is possible to override
    the default boto3 endpoints to talk to local instances of kinesis,
    dynamodb etc.

    :param service: an AWS service like "kinesis", or "dynamodb"
    :param region_name: an AWS region like "eu-west-1"
    :param resource_arn: an AWS ARN like "arn:partition:elasticache:testing:account:cluster:aws-lambda-fsm"
    :return: a tuple of service, region_name, and and possibly endpoint url
      like "http://localhost:1234"
    """
    return _get_connection_info(service, region_name, resource_arn)


def _get_connection_info(service, region_name, resource_arn):
    """
    Returns the service, region_name and endpoint_url to use when creating
    a boto3 connection. In settings.ENDPOINTS it is possible to override
    the default boto3 endpoints to talk to local instances of kinesis,
    dynamodb etc.

    :param service: an AWS service like "kinesis", or "dynamodb"
    :param region_name: an AWS region like "eu-west-1"
    :param resource_arn: an AWS ARN like "arn:partition:elasticache:testing:account:cluster:aws-lambda-fsm"
    :return: a tuple of service, region_name, and and possibly endpoint url
      like "http://localhost:1234"
    """
    endpoint_url = \
        getattr(settings, 'ENDPOINTS', {}).get(resource_arn) or \
        getattr(settings, 'ENDPOINTS', {}).get(service, {}).get(region_name) or \
        os.environ.get(service.upper() + '_URI')
    region_name = 'testing' if endpoint_url else region_name
    return service, region_name, endpoint_url


def _get_elasticache_password(cache_arn):
    """
    Returns a password to use for an Elasticache cluster or replication groupo
    with AuthTokenEnabled=True.

    :param cache_arn: an Elasticache resource ARN
    :return: a password, or None
    """
    return getattr(settings, 'ELASTICACHE_PASSWORDS', {}).get(cache_arn)


def _get_redis_connection(cache_arn, entry):
    """
    Returns a redis.StrictRedis connection to an Elasticache cluster
    or replication group.

    :param cache_arn: an Elasticache resource ARN
    :param entry: a dict returned by describe_cache_cluster, describe_replication_group
    :return: a redis.StrictRedis connection
    """
    #   {
    #     'CacheClusterId': 'def456',
    #     'TransitEncryptionEnabled': True,
    #     'AuthTokenEnabled': True,
    #     'Engine': 'redis',
    #     'CacheNodes': [
    #       {
    #         'Endpoint': {
    #           'Address': 'host',
    #           'Port': 9999
    #          }, ...
    #       }, ...
    #     }, ...
    #   }
    #
    #   {
    #     'ReplicationGroupId': 'ghi789',
    #     'TransitEncryptionEnabled': True,
    #     'AuthTokenEnabled': True,
    #     'NodeGroups': [
    #       {
    #         'PrimaryEndpoint': {
    #           'Address': 'host',
    #           'Port': 9999
    #         }, ...
    #       }
    #     ]
    #   }
    with _rlock:

        attr = 'redis_connection_for_' + cache_arn
        if not getattr(_local, attr, None):

            connection = None

            ssl = entry.get(AWS_ELASTICACHE.TransitEncryptionEnabled, False)
            password = entry.get(AWS_ELASTICACHE.AuthTokenEnabled, False) and _get_elasticache_password(cache_arn)
            host = port = None

            if AWS_ELASTICACHE.CacheClusterId in entry:
                cache_nodes = entry.get(AWS_ELASTICACHE.CacheNodes, [])
                if len(cache_nodes) == 1:
                    cache_node = cache_nodes[0]
                    endpoint = cache_node.get(AWS_ELASTICACHE.Endpoint, {})
                    host = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Address)
                    port = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Port)

            elif AWS_ELASTICACHE.ReplicationGroupId in entry:
                node_groups = entry.get(AWS_ELASTICACHE.NodeGroups, [])
                if len(node_groups) == 1:
                    node_group = node_groups[0]
                    endpoint = node_group.get(AWS_ELASTICACHE.PrimaryEndpoint, {})
                    host = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Address)
                    port = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Port)

            if host and port:
                import redis
                connection = redis.StrictRedis(host=host, port=port, db=0, ssl=ssl, password=password)

            if not connection:
                log_once(logger.fatal, "Redis Cache ARN %s is not valid.", cache_arn)

            setattr(_local, attr, connection)

        connection = getattr(_local, attr)
        return connection


def _get_memcached_connection(cache_arn, entry):
    """
    Returns a memcache.Client connection to an Elasticache cluster.

    :param cache_arn: an Elasticache resource ARN
    :param entry: a dict returned by describe_cache_cluster
    :return: a memcache.Client connection
    """

    #   {
    #     'CacheClusterId': 'abc123',
    #     'TransitEncryptionEnabled': False,
    #     'AuthTokenEnabled': False,
    #     'Engine': 'memcached',
    #     'ConfigurationEndpoint': {
    #       'Address': 'host',
    #       'Port': 9999
    #     },
    #     ...
    #   }

    with _rlock:

        attr = 'memcached_connection_for_' + cache_arn
        if not getattr(_local, attr, None):

            connection = None
            host = port = None

            if AWS_ELASTICACHE.CacheClusterId in entry:
                endpoint = entry.get(AWS_ELASTICACHE.ConfigurationEndpoint, {})
                host = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Address)
                port = endpoint.get(AWS_ELASTICACHE.ENDPOINT.Port)

            if host and port:
                import memcache
                endpoint_url = host + ":" + str(port)
                connection = memcache.Client([endpoint_url], cache_cas=True)  # memcache library does not discover nodes

            if not connection:
                log_once(logger.fatal, "Memcached Cache ARN %s is not valid.", cache_arn)

            setattr(_local, attr, connection)

        connection = getattr(_local, attr)
        return connection


def _get_elasticache_connection(cache_arn, endpoint_url):

    # if endpoint_url comes in here with a value, then it has come from
    # settings.ENDPOINTS, and we need to support it for backwards compatability
    if endpoint_url:
        entry = {
            AWS_ELASTICACHE.CacheClusterId: 'unused',
            AWS_ELASTICACHE.ConfigurationEndpoint: {
                AWS_ELASTICACHE.ENDPOINT.Address: endpoint_url.split(':')[0],
                AWS_ELASTICACHE.ENDPOINT.Port: int(endpoint_url.split(':')[1])
            }
        }
        connection = _get_memcached_connection(cache_arn, entry)

    else:
        _, connection = _get_elasticache_engine_and_connection(cache_arn)

    return connection


def _get_service_connection(resource_arn,
                            connect_timeout=DEFAULT_TIMEOUT,
                            read_timeout=DEFAULT_TIMEOUT,
                            disable_chaos=False):
    """
    Returns a connection to an AWS Service. Uses a local cache to help
    out with performance.

    :param resource_arn: an AWS resource ARN like
      'arn:partition:kinesis:region:account:resource'
    :param connect_timeout: an int socket connect timeout for api calls
    :param read_timeout: an int socket read timeout for api calls
    :param disable_chaos: a bool indicating this connection should not have any chaos
    :return: a boto3 connection
    """
    arn = get_arn_from_arn_string(resource_arn)
    with _rlock:

        # the local var is the resource arn to accommodate multiple sources in the same region
        attr = 'connection_to_' + resource_arn
        if not getattr(_local, attr, None):

            # determine the actual region_name and endpoint url if we are
            # running the services locally.
            service, region_name, endpoint_url = _get_connection_info(arn.service, arn.region_name, resource_arn)

            log_once(logger.debug, "Initializing connection for service: %s, region_name: %s, endpoint_url: %s",
                     service, region_name, endpoint_url)

            # for elasticache/memcache, we need to ensure that an actual endpoint
            # is specified, since the memcache library doesn't have all the default
            # logic used in the boto3 library
            if service == AWS.ELASTICACHE:
                connection = _get_elasticache_connection(resource_arn, endpoint_url)

            # actual AWS services with boto3 APIs
            else:
                # set the timeouts via a config
                config = Config(connect_timeout=connect_timeout,
                                read_timeout=read_timeout)
                # create a connection with the config
                connection = \
                    boto3.client(service,
                                 region_name=region_name,
                                 endpoint_url=endpoint_url,
                                 config=config)

            # wrapped in a chaos connection if applicable
            if getattr(settings, 'AWS_CHAOS', {}):
                connection = ChaosConnection(resource_arn, connection)  # pragma: no cover

            setattr(_local, attr, connection)

        connection = getattr(_local, attr)

        # if no chaos is requested, then just return the underlying boto client
        if disable_chaos and isinstance(connection, ChaosConnection):
            return connection.wrapped_connection

        return connection


def get_connection(resource_arn,
                   connect_timeout=DEFAULT_TIMEOUT,
                   read_timeout=DEFAULT_TIMEOUT,
                   disable_chaos=False):
    """
    Returns a connection to an appropriate service ARN. Since the ARN
    has the region and service encoded, it is possible to figure out all
    the appropriate connection settings.

    :param resource_arn: an AWS resource ARN like
      'arn:partition:kinesis:region:account:resource'
    :param connect_timeout: an int socket connect timeout for api calls
    :param read_timeout: an int socket read timeout for api calls
    :param disable_chaos: a bool indicating this connection should not have any chaos
    :return: a boto3 connection
    """
    connection = None
    if resource_arn:
        connection = _get_service_connection(resource_arn,
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             disable_chaos=disable_chaos)
    return connection


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


def get_secondary_cache_source():
    return settings.SECONDARY_CACHE_SOURCE


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


def get_secondary_metrics_source():
    return settings.SECONDARY_METRICS_SOURCE


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


def _set_message_dispatched_memcache(cache_arn, correlation_id, steps, retries,
                                     timeout=CACHE_DATA.CACHE_CLEANUP_TIMEOUT):
    """Sets a flag in memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    cache_key = '%s-%s' % (correlation_id, steps)
    cache_value = '%s-%s-%s' % (correlation_id, steps, retries)
    return_value = memcache_conn.set(cache_key, cache_value, time=timeout)
    return return_value


def _set_message_dispatched_redis(cache_arn, correlation_id, steps, retries,
                                  timeout=CACHE_DATA.CACHE_CLEANUP_TIMEOUT):
    """Sets a flag in redis"""
    import redis

    redis_conn = get_connection(cache_arn)
    if not redis_conn:
        return  # pragma: no cover

    try:
        cache_key = '%s-%s' % (correlation_id, steps)
        cache_value = '%s-%s-%s' % (correlation_id, steps, retries)
        return_value = redis_conn.setex(cache_key, timeout, cache_value)
        return return_value

    except redis.exceptions.ConnectionError:
        # memcache returns 0 on connectivity issues
        logger.exception('')
        return 0


def _set_message_dispatched_dynamodb(table_arn, correlation_id, steps, retries,
                                     timeout=CACHE_DATA.CACHE_CLEANUP_TIMEOUT):
    """Sets a flag in dynamodb"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    cache_key = '%s-%s' % (correlation_id, steps)
    cache_value = '%s-%s-%s' % (correlation_id, steps, retries)
    item = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
        CACHE_DATA.VALUE: {AWS_DYNAMODB.STRING: cache_value},
        CACHE_DATA.TIMEOUT: {AWS_DYNAMODB.NUMBER: str(int(time.time()) + timeout)}
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    try:
        _trace(
            dynamodb_conn.put_item,
            TableName=table_name,
            Item=item
        )
        return True

    except ClientError:
        # memcache returns 0 on connectivity issues
        logger.exception('')
        return 0


def set_message_dispatched(correlation_id, steps, retries, primary=True, timeout=CACHE_DATA.CACHE_CLEANUP_TIMEOUT):
    """
    Sets a flag in cache to indicate that a message has been dispatched.
    This is used by the framework to ensure that actions are not executed
    multiple times if the messages are received multiple times.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :param timeout: an integer representing the number of seconds-since-epoch a corresponding call
        to get_message_dispatched should return True. We allow these entries to timeout to avoid
        filling the cache with entries that will never be used again.
    :return: True if cached and False otherwise
    """
    if primary:
        source_arn = get_primary_cache_source()
    else:
        source_arn = get_secondary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No cache source for primary=%s" % primary)

    elif service == AWS.ELASTICACHE:
        engine, _ = _get_elasticache_engine_and_connection(source_arn)

        if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
            return _set_message_dispatched_memcache(source_arn, correlation_id, steps, retries, timeout=timeout)

        elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
            return _set_message_dispatched_redis(source_arn, correlation_id, steps, retries, timeout=timeout)

    elif service == AWS.DYNAMODB:
        return _set_message_dispatched_dynamodb(source_arn, correlation_id, steps, retries, timeout=timeout)


def _get_message_dispatched_memcache(cache_arn, correlation_id, steps):
    """Gets a flag from memcache"""

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return False  # pragma: no cover

    cache_key = '%s-%s' % (correlation_id, steps)
    return_value = memcache_conn.get(cache_key)
    return return_value


def _get_message_dispatched_redis(cache_arn, correlation_id, steps):
    """Gets a flag from memcache"""
    import redis

    redis_conn = get_connection(cache_arn)
    if not redis_conn:
        return False  # pragma: no cover

    try:
        cache_key = '%s-%s' % (correlation_id, steps)
        return_value = redis_conn.get(cache_key)
        return return_value

    except redis.exceptions.ConnectionError:
        # memcache returns None on connectivity issues
        logger.exception('')
        return None


def _get_message_dispatched_dynamodb(table_arn, correlation_id, steps):
    """Gets a flag from dynamodb"""

    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    cache_key = '%s-%s' % (correlation_id, steps)
    key = {
        CACHE_DATA.KEY: {AWS_DYNAMODB.STRING: cache_key},
    }

    # write the kinesis offset to dynamodb. this allows us to recover hung/incomplete fsms.
    try:
        return_value = _trace(
            dynamodb_conn.get_item,
            ConsistentRead=True,
            TableName=table_name,
            Key=key
        )

        # check if the dynamodb entry is expired
        timeout = return_value.get(AWS_DYNAMODB.Item, {}) \
                              .get(CACHE_DATA.TIMEOUT, {}) \
                              .get(AWS_DYNAMODB.NUMBER, "0")
        if int(timeout) < int(time.time()):
            # expired
            return None
        else:
            # not expired
            return return_value.get(AWS_DYNAMODB.Item, {}) \
                               .get(CACHE_DATA.VALUE, {}) \
                               .get(AWS_DYNAMODB.STRING, None)

    except ClientError:
        # memcache returns None on connectivity issues
        logger.exception('')
        return None


def get_message_dispatched(correlation_id, steps, primary=True):
    """
    Sets a flag in cache to indicate that a message has been dispatched.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :return: True if cached and False otherwise
    """
    if primary:
        source_arn = get_primary_cache_source()
    else:
        source_arn = get_secondary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No cache source for primary=%s" % primary)

    elif service == AWS.ELASTICACHE:
        engine, _ = _get_elasticache_engine_and_connection(source_arn)

        if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
            return _get_message_dispatched_memcache(source_arn, correlation_id, steps)

        elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
            return _get_message_dispatched_redis(source_arn, correlation_id, steps)

    elif service == AWS.DYNAMODB:
        return _get_message_dispatched_dynamodb(source_arn, correlation_id, steps)


def _serialize_lease_value(steps, retries, expires, fence_token):
    return '%d:%d:%d:%d' % (steps, retries, expires, fence_token)


def _deserialize_lease_value(value):
    return map(int, value.split(':'))


def _acquire_lease_memcache(cache_arn, correlation_id, steps, retries, timeout=LEASE_DATA.LEASE_TIMEOUT):
    """
    Acquires a lease from memcache.

    # https://www.quora.com/What-is-the-best-way-to-implement-a-mutex-on-top-of-memcached
    # http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html

    NOTE: This is memcached, which routinely ejects cache items, so this is clearly only
        an _advisory_ lease.
    """

    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    # the timeout is stored in the value of the cached lease, rather than relying on
    # memcache to eject expired leases. this allows us to use memcached to store an
    # advisory fence token in the current value.
    timestamp = int(time.time())
    new_expires = timestamp + timeout

    # get the current value of the lease
    memcache_key = LEASE_DATA.LEASE_KEY_PREFIX + correlation_id

    try:
        current_lease_value = memcache_conn.gets(memcache_key)

        # if there is already a lease holder, then we have a few options
        if current_lease_value:

            # split the current lease apart
            current_steps, current_retries, current_expires, current_fence_token = \
                _deserialize_lease_value(current_lease_value)

            # the existing lease has expired, forcibly take it
            if timestamp > current_expires:
                new_fence_token = current_fence_token + 1
                new_lease_value = _serialize_lease_value(steps, retries, new_expires, new_fence_token)
                success = memcache_conn.cas(memcache_key, new_lease_value, time=LEASE_DATA.LEASE_CLEANUP_TIMEOUT)
                # >>> import memcache
                # >>> c1=memcache.Client(['localhost:11211'],cache_cas=True)
                # >>> c2=memcache.Client(['localhost:11211'],cache_cas=True)
                # >>> c3=memcache.Client(['localhost:22222'],cache_cas=True)
                # >>> c1.set('abc123', 'def456')
                # True
                # >>> c1.gets('abc123')
                # 'def456'
                # >>> c2.gets('abc123')
                # 'def456'
                # >>> c2.cas('abc123', 'zzz111')
                # True
                # >>> c1.cas('abc123', 'yyy333')
                # False
                # >>> c3.cas('abc123', '123456')
                # 0
                if success is False:
                    logger.warn("Cannot acquire memcache lease: unexpectedly lost 'memcache.cas' race")
                return new_fence_token if success else success

            logger.warn("Cannot acquire memcache lease: self %s, owner %s",
                        (steps, retries), (current_steps, current_retries))

            # default fall-through is to re-try to acquire the lease
            return False

        else:

            # if there is no current lease, then get the lease and initialize the fence token
            new_fence_token = 1
            new_lease_value = _serialize_lease_value(steps, retries, new_expires, new_fence_token)
            success = memcache_conn.add(memcache_key, new_lease_value, time=LEASE_DATA.LEASE_CLEANUP_TIMEOUT)
            # >>> import memcache
            # >>> c1=memcache.Client(['localhost:11211'],cache_cas=True)
            # >>> c2=memcache.Client(['localhost:11211'],cache_cas=True)
            # >>> c3=memcache.Client(['localhost:22222'],cache_cas=True)
            # >>> c1.add('aaa','aaa')
            # True
            # >>> c2.add('aaa','aaa')
            # False
            # >>> c3.add('aaa','aaa')
            # 0
            if success is False:
                logger.warn("Cannot acquire memcache lease: unexpectedly lost 'memcache.add' race")
            return new_fence_token if success else success

    finally:
        # as per the comment in memcache.Client:
        #
        # @param cache_cas: (default False) If true, cas operations will
        # be cached.  WARNING: This cache is not expired internally, if
        # you have a long-running process you will need to expire it
        # manually via client.reset_cas(), or the cache can grow
        # unlimited.
        memcache_conn.cas_ids.pop(memcache_key, None)


def _acquire_lease_redis(cache_arn, correlation_id, steps, retries, timeout=LEASE_DATA.LEASE_TIMEOUT):
    """
    Acquires a lease from redis.

    # https://github.com/andymccurdy/redis-py
    # http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html

    "If you need locks only on a best-effort basis (as an efficiency optimization, not for correctness), I
    would recommend sticking with the straightforward single-node locking algorithm for Redis
    (conditional set-if-not-exists to obtain a lock, atomic delete-if-value-matches to release a lock),
    and documenting very clearly in your code that the locks are only approximate and may occasionally
    fail. Don't bother with setting up a cluster of five Redis nodes." - Martin Kleppmann
    """
    import redis

    redis_conn = get_connection(cache_arn)
    if not redis_conn:
        return  # pragma: no cover

    # the timeout is stored in the value of the cached lease, rather than relying on
    # redis to eject expired leases. this allows us to use redis to store an
    # advisory fence token in the current value.
    timestamp = int(time.time())
    new_expires = timestamp + timeout

    with redis_conn.pipeline() as pipe:

        try:
            # get the current value of the lease (within a WATCH)
            redis_key = LEASE_DATA.LEASE_KEY_PREFIX + correlation_id
            pipe.watch(redis_key)
            current_lease_value = pipe.get(redis_key)
            pipe.multi()

            if current_lease_value:

                # split the current lease apart
                current_steps, current_retries, current_expires, current_fence_token = \
                    _deserialize_lease_value(current_lease_value)

                # the existing lease has expired, forcibly take it
                if timestamp > current_expires:
                    new_fence_token = current_fence_token + 1
                    new_lease_value = _serialize_lease_value(steps, retries, new_expires, new_fence_token)
                    pipe.setex(redis_key, LEASE_DATA.LEASE_CLEANUP_TIMEOUT, new_lease_value)

                # default fall-through is to re-try to acquire the lease
                else:
                    logger.warn("Cannot acquire redis lease: self %s, owner %s",
                                (steps, retries), (current_steps, current_retries))
                    return False

            else:

                # if there is no current lease, then get the lease
                new_fence_token = 1
                new_lease_value = _serialize_lease_value(steps, retries, new_expires, new_fence_token)
                pipe.setex(redis_key, LEASE_DATA.LEASE_CLEANUP_TIMEOUT, new_lease_value)

            # execute the transaction
            pipe.execute()

            # if we make it this far, we now own the lease
            return new_fence_token

        except redis.WatchError:
            logger.warn("Cannot acquire redis lease: unexpectedly lost 'pipe.watch' race")
            return False

        except redis.exceptions.ConnectionError:
            logger.exception('')
            return 0


def _acquire_lease_dynamodb(table_arn, correlation_id, steps, retries, timeout=LEASE_DATA.LEASE_TIMEOUT):
    """
    Acquires a lease from DynamoDB.

    # http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    timestamp = int(time.time())
    expires = timestamp + timeout

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    key = {
        LEASE_DATA.KEY: {AWS_DYNAMODB.STRING: LEASE_DATA.LEASE_KEY_PREFIX + correlation_id}
    }

    try:
        # the conditions are:
        #
        # 1. entity doesn't exist yet, or
        # 2. the lease is currently 'open', or
        # 3. the lease has expired, or
        cexp = 'attribute_not_exists(lease_state) OR ' \
               'lease_state = :o OR ' \
               'expires < :t'

        # the updates are:
        #
        # 1. atomic increment on fence, and
        # 2. expiration in the future, and
        # 3. state to 'leased', and
        # 4. steps, and
        # 5. retries
        uexp = 'SET fence = if_not_exists(fence, :z) + :f, ' \
               'expires = :e, ' \
               'lease_state = :l, ' \
               'steps = :s, ' \
               'retries = :r' \

        expression_attribute_values = {
            # leased and open states
            ':o': {AWS_DYNAMODB.STRING: LEASE_DATA.STATES.OPEN},
            ':l': {AWS_DYNAMODB.STRING: LEASE_DATA.STATES.LEASED},

            # current timestanp for conditional expiry check
            ':t': {AWS_DYNAMODB.NUMBER: str(timestamp)},

            # increment value for the fence, and a zero
            ':f': {AWS_DYNAMODB.NUMBER: str(1)},
            ':z': {AWS_DYNAMODB.NUMBER: str(0)},

            # set the expiration every time
            ':e': {AWS_DYNAMODB.NUMBER: str(expires)},

            # set the owner parameters
            ':s': {AWS_DYNAMODB.NUMBER: str(steps)},
            ':r': {AWS_DYNAMODB.NUMBER: str(retries)}
        }

        return_value = _trace(
            dynamodb_conn.update_item,
            TableName=table_name,
            Key=key,
            ConditionExpression=cexp,
            UpdateExpression=uexp,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="ALL_NEW"
        )

        # the conditional update and atomic increment worked
        fence_token_str = return_value[AWS_DYNAMODB.Attributes][LEASE_DATA.FENCE][AWS_DYNAMODB.NUMBER]
        return int(fence_token_str)

    except ClientError, e:

        # operating as expected for entity already existing
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warn("Cannot acquire dynamodb lease: unexpectedly lost 'ConditionExpression' race")
            return False

        logger.exception('')
        return 0


def acquire_lease(correlation_id, steps, retries, primary=True, timeout=LEASE_DATA.LEASE_TIMEOUT):
    """
    Acquires a lease from cache.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :param retries: an integer corresponding to the number of retries in the fsm execution
    :param timeout: an integer representing the number of seconds-since-epoch the lease
        should remain active. in the event of system error, we want to ensure an unreleased
        lease should eventually be acquired by another process.
    :return: True if the lease was acquired, False if the lease was not acquired and 0 if
        there was some sort of systems/communication error.
    """
    if primary:
        source_arn = get_primary_cache_source()
    else:
        source_arn = get_secondary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No cache source for primary=%s" % primary)

    elif service == AWS.ELASTICACHE:
        engine, _ = _get_elasticache_engine_and_connection(source_arn)

        if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
            return _acquire_lease_memcache(source_arn, correlation_id, steps, retries, timeout=timeout)

        elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
            return _acquire_lease_redis(source_arn, correlation_id, steps, retries, timeout=timeout)

    elif service == AWS.DYNAMODB:
        return _acquire_lease_dynamodb(source_arn, correlation_id, steps, retries, timeout=timeout)


def _release_lease_memcache(cache_arn, correlation_id, steps, retries, fence_token):
    """
    Releases a lease from memcache.
    """
    memcache_conn = get_connection(cache_arn)
    if not memcache_conn:
        return  # pragma: no cover

    # get the current value of the lease
    memcache_key = LEASE_DATA.LEASE_KEY_PREFIX + correlation_id

    try:
        current_lease_value = memcache_conn.gets(memcache_key)

        # if there is already a lease holder, then we have a few options
        if current_lease_value:

            # split the current lease apart
            current_steps, current_retries, current_time, current_fence_token = \
                _deserialize_lease_value(current_lease_value)

            # >>> import memcache
            # >>> c1 = memcache.Client(['localhost:11211'],cache_cas=True)
            # >>> c2 = memcache.Client(['localhost:22222'],cache_cas=True)
            # >>> c1.set('a','a')
            # True
            # >>> c1.delete('a')
            # 1
            # >>> c1.delete('a')
            # 1
            # >>> c1.get('a')
            # >>> c2.delete('a')
            # 0

            # release it by:
            # 1. setting the lease value to "unowned" (steps/retries = -1)
            # 2. setting it as expired (expires = 0) with cas, rather than just client.delete, which can race.
            # 3. setting the fence token to the current value so it can be incremented later
            if (current_steps, current_retries, current_fence_token) == (steps, retries, fence_token):
                new_fence_token = fence_token
                new_lease_value = _serialize_lease_value(-1, -1, 0, new_fence_token)
                success = memcache_conn.cas(memcache_key, new_lease_value, time=LEASE_DATA.LEASE_CLEANUP_TIMEOUT)
                if success is False:
                    logger.warn("Cannot release memcache lease: unexpectedly lost 'memcache.cas' race")
                return success

            # otherwise, something else owns the lease, so we can't release it
            else:
                logger.warn("Cannot release memcache lease: self %s, owner %s",
                            (steps, retries), (current_steps, current_retries))
                return False

        else:

            # the lease is no longer owned by anyone
            logger.warn("Cannot release memcache lease: not owned by anyone")
            return False

    finally:
        # as per the comment in memcache.Client:
        #
        # @param cache_cas: (default False) If true, cas operations will
        # be cached.  WARNING: This cache is not expired internally, if
        # you have a long-running process you will need to expire it
        # manually via client.reset_cas(), or the cache can grow
        # unlimited.
        memcache_conn.cas_ids.pop(memcache_key, None)


def _release_lease_redis(cache_arn, correlation_id, steps, retries, fence_token):
    """
    Releases a lease from redis.
    """
    import redis

    redis_conn = get_connection(cache_arn)
    if not redis_conn:
        return  # pragma: no cover

    with redis_conn.pipeline() as pipe:

        try:

            # get the current value of the lease (within a watch)
            redis_key = LEASE_DATA.LEASE_KEY_PREFIX + correlation_id
            pipe.watch(redis_key)
            current_lease_value = pipe.get(redis_key)
            pipe.multi()

            # if there is already a lease holder, then we have a few options
            if current_lease_value:

                # split the current lease apart
                current_steps, current_retries, current_time, current_fence_token = \
                    _deserialize_lease_value(current_lease_value)

                # release it by:
                # 1. setting the lease value to "unowned" (steps/retries = -1)
                # 2. setting it as expired (expires = 0) with set
                # 3. setting the fence token to the current value so it can be incremented later
                if (current_steps, current_retries, current_fence_token) == (steps, retries, fence_token):
                    new_fence_token = fence_token
                    new_lease_value = _serialize_lease_value(-1, -1, 0, new_fence_token)
                    pipe.setex(redis_key, LEASE_DATA.LEASE_CLEANUP_TIMEOUT, new_lease_value)

                # otherwise, something else owns the lease, so we can't release it
                else:
                    logger.warn("Cannot release redis lease: self %s, owner %s",
                                (steps, retries), (current_steps, current_retries))
                    return False

            else:

                # the lease is no longer owned by anyone
                logger.warn("Cannot release redis lease: not owned by anyone")
                return False

            # execute the transaction
            pipe.execute()

            # if we make it this far, we have released the lease
            return True

        except redis.WatchError:
            logger.warn("Cannot release redis lease: unexpectedly lost 'pipe.watch' race")
            return False

        except redis.exceptions.ConnectionError:
            logger.exception('')
            return 0


def _release_lease_dynamodb(table_arn, correlation_id, steps, retries, fence_token):
    """
    Releases a lease from DynamoDB.

    # http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    key = {
        LEASE_DATA.KEY: {AWS_DYNAMODB.STRING: LEASE_DATA.LEASE_KEY_PREFIX + correlation_id}
    }

    try:
        # the conditions are:
        #
        # 1. the lease is currently 'leased', and
        # 2. steps matches, and
        # 3. retries matches, and
        # 4. fence token matches
        cexp = 'lease_state = :l AND ' \
               'steps = :s AND ' \
               'retries = :r AND ' \
               'fence = :f'

        # the updates are:
        #
        # 1. lease state to 'open', and
        # 2. null steps, and
        # 3. null retries, and
        # 4. null expires, and
        # 5. fence token current
        uexp = 'SET lease_state = :o, ' \
               'steps = :null, ' \
               'retries = :null, ' \
               'expires = :null, ' \
               'fence = :f'

        expression_attribute_values = {
            # leased and open states
            ':o': {AWS_DYNAMODB.STRING: LEASE_DATA.STATES.OPEN},
            ':l': {AWS_DYNAMODB.STRING: LEASE_DATA.STATES.LEASED},

            # null out all the other parameters
            ':null': {AWS_DYNAMODB.NULL: True},

            # used for conditional expression
            ':s': {AWS_DYNAMODB.NUMBER: str(steps)},
            ':r': {AWS_DYNAMODB.NUMBER: str(retries)},

            # fence token
            ':f': {AWS_DYNAMODB.NUMBER: str(fence_token)}
        }

        _trace(
            dynamodb_conn.update_item,
            TableName=table_name,
            Key=key,
            ConditionExpression=cexp,
            UpdateExpression=uexp,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="ALL_NEW"
        )

        # the conditional update and atomic increment worked
        return True

    except ClientError, e:

        # operating as expected for entity already existing
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warn("Cannot release dynamodb lease: unexpectedly lost 'ConditionExpression' race")
            return False

        logger.exception('')
        return 0


def release_lease(correlation_id, steps, retries, fence_token, primary=True):
    """
    Releases a lease from cache.

    :param correlation_id: a str guid for the fsm
    :param steps: an integer corresponding to the step in the fsm execution
    :param retries: an integer corresponding to the number of retries in the fsm execution
    :return: True if the lease was released, False if the lease was not released and 0 if
        there was some sort of systems/communication error.
    """
    if primary:
        source_arn = get_primary_cache_source()
    else:
        source_arn = get_secondary_cache_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No cache source for primary=%s" % primary)

    elif service == AWS.ELASTICACHE:
        engine, _ = _get_elasticache_engine_and_connection(source_arn)

        if engine == AWS_ELASTICACHE.ENGINE.MEMCACHED:
            return _release_lease_memcache(source_arn, correlation_id, steps, retries, fence_token)

        elif engine == AWS_ELASTICACHE.ENGINE.REDIS:
            return _release_lease_redis(source_arn, correlation_id, steps, retries, fence_token)

    elif service == AWS.DYNAMODB:
        return _release_lease_dynamodb(source_arn, correlation_id, steps, retries, fence_token)


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

    stream_name = get_arn_from_arn_string(stream_arn).slash_resource()
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
    table_name = get_arn_from_arn_string(table_arn).slash_resource()
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
        Message=data
    )
    return return_value


def _get_sqs_queue_url(queue_arn):
    """
    Returns the SQS queue URL for the given ARN. SQS urls are not guaranteed to be easily
    derivable from SQS ARNs, so we make a service call to make sure we have the correct
    value.

    :param queue_arn: an SQS queue ARN like 'arn:partition:sqs:testing:account:aws-lambda-fsm'
    :return: a url like 'https://sqs.testing.amazonaws.com/account/aws-lambda-fsm'
    """
    arn = get_arn_from_arn_string(queue_arn)

    # first lookup via settings.SQS_URLS data
    if queue_arn in getattr(settings, 'SQS_URLS', {}):
        return settings.SQS_URLS.get(queue_arn, {}).get(AWS_SQS.QueueUrl)

    sqs_conn = get_connection(queue_arn)
    if not sqs_conn:
        return  # pragma: no cover

    # since this makes an external call, which may be expensive, we don't bother
    # locking like in other locations where _local is mutated. the url never changes
    # so looking it up in a couple threads simultaneously does not harm. also
    # aws lambda dispatch model doesn't appear to use multiple threads anyway.

    attr = 'url_for_' + queue_arn
    if not getattr(_local, attr, None):

        log_once(logger.warning, 'Consider using settings.SQS_URLS for urls.')

        return_value = _trace(
            sqs_conn.get_queue_url,
            QueueName=arn.resource,
            QueueOwnerAWSAccountId=arn.account_id
        )

        # check that we were able to lookup the queue
        if AWS_SQS.QueueUrl not in return_value:
            log_once(logger.fatal, "Queue ARN %s does not exist.", queue_arn)

        url = return_value.get(AWS_SQS.QueueUrl)

        setattr(_local, attr, url)

    return getattr(_local, attr)


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

    queue_url = _get_sqs_queue_url(queue_arn)
    return_value = _trace(
        sqs_conn.send_message,
        QueueUrl=queue_url,
        MessageBody=data,
        DelaySeconds=delay
    )
    return return_value


def send_next_event_for_dispatch(context, data, correlation_id, delay=0, primary=True, recovering=False):
    """
    Sends an FSM event message onto Kinesis or DynamoDB or SNS.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param data: a str data for the sns message
    :param correlation_id: the guid for the fsm
    :param primary: if True, use the primary stream source, and if False
      use the secondary stream source
    :param recovering: if True, use the primary retry source, and if False
      use the secondary retry source
    :return: see above.
    """
    if primary:
        if recovering:
            source_arn = get_primary_retry_source()
        else:
            source_arn = get_primary_stream_source()
    else:
        if recovering:
            source_arn = get_secondary_retry_source()
        else:
            source_arn = get_secondary_stream_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No stream source for primary=%s" % primary)

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

    stream_name = get_arn_from_arn_string(stream_arn).slash_resource()
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

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
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

    queue_url = _get_sqs_queue_url(queue_arn)
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
        log_once(logger.warning, "No stream source for primary=%s" % primary)

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

    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    item = {
        CHECKPOINT_DATA.CORRELATION_ID: {AWS_DYNAMODB.STRING: correlation_id},
        CHECKPOINT_DATA.SENT: {AWS_DYNAMODB.STRING: sent}
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

    table_name = get_arn_from_arn_string(table_arn).slash_resource()

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
        log_once(logger.warning, "No environment source for primary=%s" % primary)

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

    table_name = get_arn_from_arn_string(table_arn).slash_resource()

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
        log_once(logger.warning, "No environment source for primary=%s" % primary)

    if service == AWS.DYNAMODB:
        return _load_environment_dynamodb(source, guid)


def _start_retries_dynamodb(table_arn, correlation_id, steps, run_at, payload):
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
    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    correlation_id_steps = '%s-%s' % (correlation_id, steps)
    item = {
        RETRY_DATA.PARTITION: {AWS_DYNAMODB.NUMBER: str(partition)},
        RETRY_DATA.CORRELATION_ID_STEPS: {AWS_DYNAMODB.STRING: correlation_id_steps},
        RETRY_DATA.RUN_AT: {AWS_DYNAMODB.NUMBER: str(run_at)},
        RETRY_DATA.PAYLOAD: {AWS_DYNAMODB.STRING: payload}
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
    queue_url = _get_sqs_queue_url(queue_arn)
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


def start_retries(context, run_at, payload, primary=True, recovering=False):
    """
    Triggers retries for a state machine by sending a message to a "run_at"
    parameter designating when to run the retry.

    :param context: a aws_lambda_fsm.fsm.Context instance
    :param run_at: a integer time since epoch
    :param payload: the retry payload (serialized fsm context)
    :param primary: if True, use the primary retries source, and if False
      use the retries environment source
    :param recovering: if True, use the primary stream source, and if False
      use the secondary stream source
    :return: see above.
    """
    if primary:
        if recovering:
            source_arn = get_primary_stream_source()
        else:
            source_arn = get_primary_retry_source()
    else:
        if recovering:
            source_arn = get_secondary_stream_source()
        else:
            source_arn = get_secondary_retry_source()

    service = get_arn_from_arn_string(source_arn).service

    if not service:  # pragma: no cover
        log_once(logger.warning, "No retry source for primary=%s" % primary)

    elif service == AWS.KINESIS:
        return _start_retries_kinesis(source_arn, context.correlation_id, payload)

    elif service == AWS.DYNAMODB:
        return _start_retries_dynamodb(source_arn, context.correlation_id, context.steps, run_at, payload)

    elif service == AWS.SNS:
        return _start_retries_sns(source_arn, context.correlation_id, payload)

    elif service == AWS.SQS:
        return _start_retries_sqs(source_arn, context.correlation_id, run_at, payload)


def _stop_retries_dynamodb(table_arn, correlation_id, steps):
    """
    Stops retries for a state machine by deleting any persistent messages
    that trigger retires.

    :param table_arn: a str ARN for a DynamoDB table like
      'arn:partition:dynamodb:region:account:resource'
    :param correlation_id: the guid for the fsm
    :param steps: the steps of the fsm
    :return: a return value from boto3 delete_item call
    """
    dynamodb_conn = get_connection(table_arn)
    if not dynamodb_conn:
        return  # pragma: no cover

    partition = int(hashlib.md5(correlation_id).hexdigest(), 16) % 16
    table_name = get_arn_from_arn_string(table_arn).slash_resource()
    correlation_id_steps = '%s-%s' % (correlation_id, steps)
    key = {
        RETRY_DATA.PARTITION: {AWS_DYNAMODB.NUMBER: str(partition)},
        RETRY_DATA.CORRELATION_ID_STEPS: {AWS_DYNAMODB.STRING: correlation_id_steps}
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
        log_once(logger.warning, "No retry source for primary=%s" % primary)

    elif service == AWS.DYNAMODB:
        return _stop_retries_dynamodb(source_arn, context.correlation_id, context.steps)


def retriable_entities(table_arn, index, run_at, limit=100):
    """

    :param table:
    :param index:
    :param run_at:
    :param limit:
    :return:
    """
    # query for some dynamodb entities
    dynamodb_conn = get_connection(table_arn, disable_chaos=True)
    if not dynamodb_conn:
        return []

    items = []

    table_name = get_arn_from_arn_string(table_arn).slash_resource()

    for partition in xrange(16):

        # query by partition
        results = _trace(
            dynamodb_conn.query,
            TableName=table_name,
            ConsistentRead=True,
            IndexName=index,
            KeyConditions={
                RETRY_DATA.PARTITION: {
                    AWS_DYNAMODB.ComparisonOperator: AWS_DYNAMODB.EQUAL,
                    AWS_DYNAMODB.AttributeValueList: [{AWS_DYNAMODB.NUMBER: str(partition)}]
                },
                RETRY_DATA.RUN_AT: {
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
                    RETRY_DATA.PAYLOAD: result[RETRY_DATA.PAYLOAD][AWS_DYNAMODB.STRING],
                    RETRY_DATA.CORRELATION_ID_STEPS: result[RETRY_DATA.CORRELATION_ID_STEPS][AWS_DYNAMODB.STRING],
                }
            )

    return items

################################################################################
# Configuration Validation
################################################################################


ALLOWED_STREAM_SERVICES = [AWS.KINESIS, AWS.DYNAMODB, AWS.SNS, AWS.SQS]
ALLOWED_RETRY_SERVICES = [AWS.KINESIS, AWS.DYNAMODB, AWS.SNS, AWS.SQS]
ALLOWED_CHECKPOINT_SERVICES = [AWS.DYNAMODB]
ALLOWED_ENVIRONMENT_SERVICES = [AWS.DYNAMODB]
ALLOWED_METRICS_SERVICES = [AWS.CLOUDWATCH]
ALLOWED_CACHE_SERVICES = [AWS.ELASTICACHE, AWS.DYNAMODB]


ALLOWED = 'allowed'
PRIMARY = 'primary'
SECONDARY = 'secondary'
FAILOVER = 'failover'
REQUIRED = 'required'


ALLOWED_MAPPING = {

    # required and support failover
    'STREAM': {
        ALLOWED: ALLOWED_STREAM_SERVICES,
        REQUIRED: True,
        FAILOVER: True,
        PRIMARY: get_primary_stream_source(),
        SECONDARY: get_secondary_stream_source(),
    },
    'RETRY': {
        ALLOWED: ALLOWED_RETRY_SERVICES,
        REQUIRED: True,
        FAILOVER: True,
        PRIMARY: get_primary_retry_source(),
        SECONDARY: get_secondary_retry_source(),
    },
    'CACHE': {
        ALLOWED: ALLOWED_CACHE_SERVICES,
        REQUIRED: True,
        FAILOVER: True,
        PRIMARY: get_primary_cache_source(),
        SECONDARY: get_secondary_cache_source(),
    },

    # not required and do not support failover
    'CHECKPOINT': {
        ALLOWED: ALLOWED_CHECKPOINT_SERVICES,
        REQUIRED: False,
        FAILOVER: False,
        PRIMARY: get_primary_checkpoint_source(),
        SECONDARY: get_secondary_checkpoint_source(),
    },

    'ENVIRONMENT': {
        ALLOWED: ALLOWED_ENVIRONMENT_SERVICES,
        REQUIRED: False,
        FAILOVER: False,
        PRIMARY: get_primary_environment_source(),
        SECONDARY: get_secondary_environment_source(),
    },
    'METRICS': {
        ALLOWED: ALLOWED_METRICS_SERVICES,
        REQUIRED: False,
        FAILOVER: False,
        PRIMARY: get_primary_metrics_source(),
        SECONDARY: get_secondary_metrics_source(),
    },
}


def _validate_config(key, data):
    """
    Validates the settings/config. Logs errors when problems are found.
    """
    primary = data[PRIMARY]
    secondary = data[SECONDARY]
    allowed = data[ALLOWED]
    failover = data[FAILOVER]
    required = data[REQUIRED]

    if required and not primary:
        log_once(logger.fatal, "PRIMARY_%s_SOURCE is unset.", key)

    primary_service = get_arn_from_arn_string(primary).service
    if primary_service and primary_service not in allowed:
        log_once(logger.fatal, "PRIMARY_%s_SOURCE '%s' is not allowed.", key, primary)

    secondary_service = get_arn_from_arn_string(secondary).service
    if secondary_service and secondary_service not in allowed:
        log_once(logger.fatal, "SECONDARY_%s_SOURCE '%s' is not allowed.", key, secondary)

    if failover and not secondary:
        log_once(logger.warning, "SECONDARY_%s_SOURCE is unset (failover not configured).", key)

    if failover and secondary and primary == secondary:
        log_once(logger.warning,
                 "PRIMARY_%s_SOURCE = SECONDARY_%s_SOURCE (failover not configured optimally).", key, key)


def _validate_sqs_urls():
    """
    Validates settings.SQS_URLS is correctly formed

    SQS_URLS = {
      "queue_arn1": {
        "QueueUrl": "https://address/queue1"
      },
      "queue_arn2": {
        "QueueUrl": "https://address/queue2"
      }
    }

    """
    if hasattr(settings, 'SQS_URLS'):
        for queue_arn, entry in settings.SQS_URLS.iteritems():
            arn = get_arn_from_arn_string(queue_arn)
            if arn.service != AWS.SQS:
                log_once(logger.warning, "SQS_URLS has invalid key '%s' (service)", queue_arn)
            if AWS_SQS.QueueUrl not in entry:
                log_once(logger.warning, "SQS_URLS has invalid entry for key '%s' (url)", queue_arn)


def _validate_endpoint(cache_arn, endpoint):

    if AWS_ELASTICACHE.ENDPOINT.Address not in endpoint:
        log_once(logger.warning, "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (address)", cache_arn)
    if AWS_ELASTICACHE.ENDPOINT.Port not in endpoint:
        log_once(logger.warning, "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (port)", cache_arn)


def _validate_elasticache_endpoints():
    """
    Validates settings.ELASTICACHE_ENDPOINTS is correctly formed

    ELASTICACHE_ENDPOINTS = {
       'memcached_cache_cluster_arn': {
         'CacheClusterId': 'abc123',
         'TransitEncryptionEnabled': False,
         'AuthTokenEnabled': False,
         'Engine': 'memcached',
         'ConfigurationEndpoint': {
           'Address': 'host',
           'Port': 9999
         },  ...
       },

       'redis_cache_cluster_arn': {
         'CacheClusterId': 'def456',
         'TransitEncryptionEnabled': True,
         'AuthTokenEnabled': True,
         'Engine': 'redis',
         'CacheNodes': [
           {
             'Endpoint': {
               'Address': 'host',
               'Port': 9999
              }, ...
           }, ...
         }, ...
       },

       'redis_replication_group_arn': {
         'ReplicationGroupId': 'ghi789',
         'TransitEncryptionEnabled': True,
         'AuthTokenEnabled': True,
         'NodeGroups': [
           {
             'PrimaryEndpoint': {
               'Address': 'host',
               'Port': 9999
             }, ...
           }
         ]
       }
    }
    """
    if hasattr(settings, 'ELASTICACHE_ENDPOINTS'):
        for cache_arn, entry in settings.ELASTICACHE_ENDPOINTS.iteritems():
            arn = get_arn_from_arn_string(cache_arn)
            if arn.service != AWS.ELASTICACHE:
                log_once(logger.warning, "ELASTICACHE_ENDPOINTS has invalid key '%s'", cache_arn)

            # memcached and redis cache clusters
            if AWS_ELASTICACHE.CacheClusterId in entry:

                # needs an engine
                if AWS_ELASTICACHE.Engine not in entry:
                    log_once(logger.warning, "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (engine)", cache_arn)

                else:
                    # memcached cluster
                    if entry[AWS_ELASTICACHE.Engine] == AWS_ELASTICACHE.ENGINE.MEMCACHED:

                        if AWS_ELASTICACHE.ConfigurationEndpoint not in entry:
                            log_once(logger.warning,
                                     "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (endpoint)", cache_arn)
                        else:
                            endpoint = entry.get(AWS_ELASTICACHE.ConfigurationEndpoint, {})
                            _validate_endpoint(cache_arn, endpoint)

                    # redis cluster
                    elif entry[AWS_ELASTICACHE.Engine] == AWS_ELASTICACHE.ENGINE.REDIS:

                        if AWS_ELASTICACHE.CacheNodes not in entry or not entry[AWS_ELASTICACHE.CacheNodes]:
                            log_once(logger.warning,
                                     "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (cache nodes)", cache_arn)

                        else:
                            node = entry[AWS_ELASTICACHE.CacheNodes][0]

                            if AWS_ELASTICACHE.Endpoint not in node:
                                log_once(logger.warning,
                                         "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (endpoint)", cache_arn)
                            else:
                                endpoint = node.get(AWS_ELASTICACHE.Endpoint, {})
                                _validate_endpoint(cache_arn, endpoint)

                    else:
                        log_once(logger.warning,
                                 "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (unknown engine)", cache_arn)

            # redis replication groups
            elif AWS_ELASTICACHE.ReplicationGroupId in entry:

                if AWS_ELASTICACHE.NodeGroups not in entry:
                    log_once(logger.warning,
                             "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (node groups)", cache_arn)

                else:
                    node = entry[AWS_ELASTICACHE.NodeGroups][0]

                    if AWS_ELASTICACHE.PrimaryEndpoint not in node:
                        log_once(logger.warning,
                                 "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (endpoint)", cache_arn)
                    else:
                        endpoint = node.get(AWS_ELASTICACHE.PrimaryEndpoint, {})
                        _validate_endpoint(cache_arn, endpoint)

            # not a cluster or replication groups
            else:
                log_once(logger.warning,
                         "ELASTICACHE_ENDPOINTS has invalid entry for key '%s' (cache type)", cache_arn)


def _validate_cache():
    """
    Validates the cache settings
    """
    def inner(key):
        source_arn = {
            'PRIMARY': get_primary_cache_source(),
            'SECONDARY': get_secondary_cache_source()
        }[key]
        if source_arn:
            arn = get_arn_from_arn_string(source_arn)
            if arn.service == AWS.ELASTICACHE:
                log_once(logger.warning, "%s_CACHE_SOURCE supports only _advisory_ locks", key)

    inner('PRIMARY')
    inner('SECONDARY')


def validate_config():
    """
    Validates the settings/config. Logs errors when problems are found.
    """
    with _rlock:
        if not getattr(_local, 'validated_config', None):
            for key, data in sorted(ALLOWED_MAPPING.items()):
                _validate_config(key, data)
            _validate_sqs_urls()
            _validate_elasticache_endpoints()
            _validate_cache()
        _local.validated_config = True
