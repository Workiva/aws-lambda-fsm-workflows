from aws_lambda_fsm.config import set_settings


class TestSettings(object):
    """
    Settings for unit tests.
    """
    USE_KINESIS = True
    USE_DYNAMODB = True
    USE_SNS = True
    USE_ECS = True
    USE_CLOUDWATCH = True
    USE_ELASTICACHE = True
    PRIMARY_METRICS_SOURCE = 'arn:partition:cloudwatch:testing:account:resource'
    SECONDARY_METRICS_SOURCE = None
    PRIMARY_CACHE_SOURCE = 'arn:partition:elasticache:testing:account:resource'
    SECONDARY_CACHE_SOURCE = None
    PRIMARY_STREAM_SOURCE = 'arn:partition:kinesis:testing:account:stream/resource'
    SECONDARY_STREAM_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    PRIMARY_CHECKPOINT_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_CHECKPOINT_SOURCE = None
    PRIMARY_RETRY_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_RETRY_SOURCE = 'arn:partition:kinesis:testing:account:stream/resource'
    PRIMARY_ENVIRONMENT_SOURCE = 'arn:partition:dynamodb:testing:account:table/resource'
    SECONDARY_ENVIRONMENT_SOURCE = None
    AWS_CHAOS = {}
    ENDPOINTS = {
        'kinesis': {
            'testing': 'invalid_endpoint'
        },
        'dynamodb': {
            'testing': 'invalid_endpoint'
        },
        'elasticache': {
            'testing': 'invalid_endpoint'
        },
        'sns': {
            'testing': 'invalid_endpoint'
        },
        'cloudwatch': {
            'testing': 'invalid_endpoint'
        }
    }

set_settings(TestSettings)
