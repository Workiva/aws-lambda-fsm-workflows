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

################################################################################
# FSM Data Related
################################################################################


class ENVIRONMENT(object):
    FSM_CONTEXT = 'FSM_CONTEXT'
    FSM_PRIMARY_STREAM_SOURCE = 'FSM_PRIMARY_STREAM_SOURCE'
    FSM_SECONDARY_STREAM_SOURCE = 'FSM_SECONDARY_STREAM_SOURCE'
    FSM_ENVIRONMENT_GUID_KEY = 'FSM_ENVIRONMENT_GUID_KEY'
    FSM_DOCKER_IMAGE = 'FSM_DOCKER_IMAGE'


class PAYLOAD(object):
    VERSION = 'version'
    DEFAULT_VERSION = '0.1'
    SYSTEM_CONTEXT = 'system_context'
    USER_CONTEXT = 'user_context'


class SYSTEM_CONTEXT(object):
    MACHINE_NAME = 'machine_name'
    CURRENT_STATE = 'current_state'
    CURRENT_EVENT = 'current_event'
    CORRELATION_ID = 'correlation_id'
    STEPS = 'steps'
    RETRIES = 'retries'
    MAX_RETRIES = 'max_retries'
    RESTARTED_AT = 'restarted_at'
    STARTED_AT = 'started_at'
    FINISHED_AT = 'finished_at'
    STREAM = 'stream'
    TABLE = 'table'
    TOPIC = 'topic'
    METRICS = 'metrics'
    LEASE_PRIMARY = 'lease_primary'


class OBJ(object):
    PAYLOAD = 'payload'
    SENT = 'sent'
    CONTEXT = 'context'
    SOURCE = 'source'
    DELAY = 'delay'
    FENCE_TOKEN = 'fence_token'
    LAMBDA_RECORD = 'lambda_record'


class ERRORS(object):
    ERROR = 'error'
    FATAL = 'fatal'
    CACHE = 'cache'
    RETRY = 'retry'
    DUPLICATE = 'duplicate'
    DISPATCH = 'dispatch'


################################################################################
# FSM Related
################################################################################

class STATE(object):
    PSEUDO_INIT = 'pseudo_init'
    PSEUDO_FINAL = 'pseudo_final'


class CONFIG(object):
    MACHINES = 'machines'
    NAME = 'name'
    STREAM = 'stream'
    TABLE = 'table'
    METRICS = 'metrics'
    IMPORT = 'import'
    DO_ACTION = 'do_action'
    ENTRY_ACTION = 'entry_action'
    EXIT_ACTION = 'exit_action'
    INITIAL = 'initial'
    FINAL = 'final'
    TRANSITIONS = 'transitions'
    STATES = 'states'
    ACTION = 'action'
    EVENT = 'event'
    TARGET = 'target'
    TOPIC = 'topic'
    MAX_RETRIES = 'max_retries'
    DEFAULT_MAX_RETRIES = 5


class MACHINE(object):
    MACHINES = 'machines'
    STATES = 'states'
    TRANSITIONS = 'transitions'
    STREAM = 'stream'
    TABLE = 'table'
    METRICS = 'metrics'
    TOPIC = 'topic'
    MAX_RETRIES = 'max_retries'


################################################################################
# AWS Data Related
################################################################################

class STREAM_DATA(object):
    CORRELATION_ID = 'correlation_id'
    PAYLOAD = 'payload'
    TIMESTAMP = 'timestamp'


class RETRY_DATA(object):
    PARTITION = 'partition'
    CORRELATION_ID_STEPS = 'correlation_id_steps'
    RUN_AT = 'run_at'
    PAYLOAD = 'payload'
    RETRIES = 'retries'


class CHECKPOINT_DATA(object):
    CORRELATION_ID = 'correlation_id'
    SENT = 'sent'


class ENVIRONMENT_DATA(object):
    GUID = 'guid'
    ENVIRONMENT = 'environment'


class CACHE_DATA(object):
    KEY = 'ckey'
    VALUE = 'value'
    TIMEOUT = 'timeout'
    CACHE_CLEANUP_TIMEOUT = 24 * 60 * 60  # daily


class LEASE_DATA(object):
    LEASE_TIMEOUT = 5 * 60
    LEASE_CLEANUP_TIMEOUT = 24 * 60 * 60  # daily
    KEY = 'ckey'
    STATE = 'state'
    FENCE = 'fence'
    EXPIRES = 'expires'
    LEASE_KEY_PREFIX = 'lease-'

    class STATES(object):
        LEASED = 'leased'
        OPEN = 'open'


################################################################################
# AWS Related
################################################################################


class AWS(object):
    KINESIS = 'kinesis'
    DYNAMODB = 'dynamodb'
    DYNAMODB_STREAM = 'dynamodb_stream'
    DYNAMODB_RETRY = 'dynamodb_retry'
    MEMCACHE = 'memcache'
    ELASTICACHE = 'elasticache'
    CLOUDWATCH = 'cloudwatch'
    SNS = 'sns'
    ECS = 'ecs'
    GATEWAY = 'gateway'
    SQS = 'sqs'
    STEP_FUNCTION = 'step_function'


class AWS_ELASTICACHE(object):

    CacheClusters = 'CacheClusters'
    ReplicationGroups = 'ReplicationGroups'
    Engine = 'Engine'
    ConfigurationEndpoint = 'ConfigurationEndpoint'
    Endpoint = 'Endpoint'
    PrimaryEndpoint = 'PrimaryEndpoint'
    CacheClusterId = 'CacheClusterId'
    ReplicationGroupId = 'ReplicationGroupId'
    TransitEncryptionEnabled = 'TransitEncryptionEnabled'
    AuthTokenEnabled = 'AuthTokenEnabled'
    CacheNodes = 'CacheNodes'
    NodeGroups = 'NodeGroups'

    class RESOURCE_TYPE:

        CLUSTER = 'cluster'
        SNAPSHOT = 'snapshot'

    class ENDPOINT(object):

        Address = 'Address'
        Port = 'Port'

    class ENGINE(object):

        REDIS = 'redis'
        MEMCACHED = 'memcached'
        ALL = [REDIS, MEMCACHED]


class AWS_ECS(object):

    class CONTAINER_OVERRIDES(object):

        KEY = 'containerOverrides'
        CONTAINER_NAME = 'name'

        class ENVIRONMENT(object):

            KEY = 'environment'
            NAME = 'name'
            VALUE = 'value'


class AWS_CLOUDWATCH(object):
    MetricName = 'MetricName'
    Dimensions = 'Dimensions'
    Timestamp = 'Timestamp'
    Value = 'Value'
    Name = 'Name'


class AWS_KINESIS(object):
    Records = 'Records'

    class RECORD(object):
        Data = 'Data'
        PartitionKey = 'PartitionKey'
        SequenceNumber = 'SequenceNumber'

    AT_SEQUENCE_NUMBER = 'AT_SEQUENCE_NUMBER'
    ShardIterator = 'ShardIterator'
    NextShardIterator = 'NextShardIterator'
    LATEST = 'LATEST'
    StreamNames = 'StreamNames'
    StreamDescription = 'StreamDescription'
    MillisBehindLatest = 'MillisBehindLatest'

    class STREAM(object):
        Shards = 'Shards'


class AWS_SNS(object):
    Topics = 'Topics'
    NextToken = 'NextToken'
    Message = 'Message'

    class TOPIC(object):
        TopicArn = 'TopicArn'


class AWS_SQS(object):
    Messages = 'Messages'
    QueueUrl = 'QueueUrl'

    class MESSAGE(object):
        MessageBody = 'MessageBody'
        Body = 'Body'
        ReceiptHandle = 'ReceiptHandle'
        Id = 'Id'
        DelaySeconds = 'DelaySeconds'

    MAX_DELAY_SECONDS = 900


class AWS_DYNAMODB(object):
    AttributeName = 'AttributeName'
    AttributeType = 'AttributeType'
    Attributes = 'Attributes'
    NUMBER = 'N'
    STRING = 'S'
    BOOLEAN = 'BOOL'
    NULL = 'NULL'
    EQUAL = 'EQ'
    LESS_THAN = 'LT'
    GREATER_THAN = 'GT'
    KeyType = 'KeyType'
    KeySchema = 'KeySchema'
    HASH = 'HASH'
    RANGE = 'RANGE'
    IndexName = 'IndexName'
    Projection = 'Projection'
    ProjectionType = 'ProjectionType'
    ALL = 'ALL'
    ReadCapacityUnits = 'ReadCapacityUnits'
    WriteCapacityUnites = 'WriteCapacityUnits'
    ComparisonOperator = 'ComparisonOperator'
    AttributeValueList = 'AttributeValueList'
    Items = 'Items'
    Item = 'Item'
    PutRequest = 'PutRequest'


class AWS_LAMBDA(object):
    Records = 'Records'
    EventSource = 'eventSource'
    EventSourceCaps = 'EventSource'
    Source = 'source'
    REDACTED = '[REDACTED]'

    class EVENT_SOURCE(object):
        KINESIS = 'aws:kinesis'
        DYNAMODB = 'aws:dynamodb'
        SNS = 'aws:sns'
        SQS = 'aws:sqs'

    class SOURCE(object):
        EVENTS = 'aws.events'

    class KINESIS_RECORD(object):
        KINESIS = 'kinesis'
        DATA = 'data'

    class DYNAMODB_RECORD(object):
        DYNAMODB = 'dynamodb'
        NewImage = 'NewImage'

    class SNS_RECORD(object):
        SNS = 'Sns'
        Message = 'Message'
        DEFAULT = 'default'

    class SQS_RECORD(object):
        SQS = 'sqs'
        BODY = 'body'
