# AWS Kinesis settings
USE_KINESIS = True

# AWS DynamoDB settings
USE_DYNAMODB = True

# AWS SNS settings
USE_SNS = True

# AWS ECS settings
USE_ECS = True

# AWS CloudWatch settings
USE_CLOUDWATCH = True

# Memcache settings
USE_ELASTICACHE = True

# used to dictate the primary location for metrics
# valid services = cloudwatch
PRIMARY_METRICS_SOURCE = 'arn:partition:cloudwatch:testing:account:aws-lambda-fsm'
SECONDARY_METRICS_SOURCE = None  # NOT SUPPORTED YET

# used to dictate the primary location for caching
# valid services = elasticache
PRIMARY_CACHE_SOURCE = 'arn:partition:elasticache:testing:account:aws-lambda-fsm'
SECONDARY_CACHE_SOURCE = None  # NOT SUPPORTED YET

# used to dictate the primary location for event dispatch
# valid services = kinesis, dynamodb, sns
PRIMARY_STREAM_SOURCE = 'arn:partition:kinesis:testing:account:stream/aws-lambda-fsm'
SECONDARY_STREAM_SOURCE = 'arn:partition:dynamodb:testing:account:table/aws-lambda-fsm.stream'

# used to dictate the primary location for checkpointing
# valid services = dynamodb
PRIMARY_CHECKPOINT_SOURCE = 'arn:partition:dynamodb:testing:account:table/aws-lambda-fsm.checkpoint'
SECONDARY_CHECKPOINT_SOURCE = None

# used to dictate the primary location for retries
# valid services = dynamodb, kinesis, sns
PRIMARY_RETRY_SOURCE = 'arn:partition:dynamodb:testing:account:table/aws-lambda-fsm.retries'
SECONDARY_RETRY_SOURCE = 'arn:partition:kinesis:testing:account:stream/aws-lambda-fsm'

# used to dictate the primary location for environment storage
# valid services = dynamodb
PRIMARY_ENVIRONMENT_SOURCE = 'arn:partition:dynamodb:testing:account:table/aws-lambda-fsm'
SECONDARY_ENVIRONMENT_SOURCE = None

AWS_CHAOS = {}
ENDPOINTS = {}

try:
    import settingslocal
except ImportError:
    settingslocal = None

if settingslocal:
    for setting in dir(settingslocal):
        globals()[setting.upper()] = getattr(settingslocal, setting)
