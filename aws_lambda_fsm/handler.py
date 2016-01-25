# system imports
import base64
import json
import logging
import time

# application imports
from aws_lambda_fsm.fsm import Context
from aws_lambda_fsm.fsm import FSM  # noqa
from aws_lambda_fsm.aws import retriable_entities
from aws_lambda_fsm.aws import get_primary_retry_source
from aws_lambda_fsm.aws import get_arn_from_arn_string
from aws_lambda_fsm.constants import OBJ
from aws_lambda_fsm.constants import STATE
from aws_lambda_fsm.constants import AWS_LAMBDA
from aws_lambda_fsm.constants import SYSTEM_CONTEXT
from aws_lambda_fsm.constants import RECOVERY_DATA
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import STREAM_DATA
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


def _process_payload(payload_str, obj):
    """

    :param encoded:
    :param obj:
    :param lambda_context:
    :return:
    """
    payload = json.loads(payload_str)
    logger.info('payload=%s', payload)
    obj[OBJ.PAYLOAD] = payload_str
    fsm = Context.from_payload_dict(payload)
    current_event = fsm.system_context().get(SYSTEM_CONTEXT.CURRENT_EVENT, STATE.PSEUDO_INIT)
    fsm.dispatch(current_event, obj)


def lambda_api_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event:
    :return:
    """
    try:
        obj = {OBJ.SOURCE: AWS.GATEWAY}
        payload = json.dumps(lambda_event)  # API Gateway just passes straight though
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        logger.exception('Critical error handling lambda: %s', lambda_event)


def lambda_kinesis_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event:
    """
    if lambda_event[AWS_LAMBDA.Records]:
        logger.info('Processing %d records from kinesis...', len(lambda_event[AWS_LAMBDA.Records]))

    for record in lambda_event[AWS_LAMBDA.Records]:

        try:
            obj = {OBJ.SOURCE: AWS.KINESIS}
            encoded = record[AWS_LAMBDA.KINESIS_RECORD.KINESIS][AWS_LAMBDA.KINESIS_RECORD.DATA]
            payload = base64.b64decode(encoded)
            _process_payload(payload, obj)

        # in batch mode, we don't want a single error to cause the the entire batch
        # to retry. for that reason, we have opted to gobble all the errors here
        # and handle retries withing the fsm dispatch code.
        except Exception:
            logger.exception('Critical error handling record: %s', record)


def lambda_dynamodb_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event:
    """
    if lambda_event[AWS_LAMBDA.Records]:
        logger.info('Processing %d records from dynamodb updates...', len(lambda_event[AWS_LAMBDA.Records]))

    for record in lambda_event[AWS_LAMBDA.Records]:

        try:
            obj = {OBJ.SOURCE: AWS.DYNAMODB_STREAM}
            dynamodb = record[AWS_LAMBDA.DYNAMODB_RECORD.DYNAMODB]
            new_image = dynamodb[AWS_LAMBDA.DYNAMODB_RECORD.NewImage]
            payload = new_image[STREAM_DATA.PAYLOAD][AWS_DYNAMODB.STRING]
            _process_payload(payload, obj)

        # in batch mode, we don't want a single error to cause the the entire batch
        # to retry. for that reason, we have opted to gobble all the errors here
        # and handle retries withing the fsm dispatch code.
        except Exception:
            logger.exception('Critical error handling record: %s', record)


def lambda_sns_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event:
    """
    if lambda_event[AWS_LAMBDA.Records]:
        logger.info('Processing %d records from sns updates...', len(lambda_event[AWS_LAMBDA.Records]))

    for record in lambda_event[AWS_LAMBDA.Records]:

        try:
            obj = {OBJ.SOURCE: AWS.SNS}
            sns = record[AWS_LAMBDA.SNS_RECORD.SNS]
            message = sns[AWS_LAMBDA.SNS_RECORD.Message]
            payload = json.loads(message)[AWS_LAMBDA.SNS_RECORD.DEFAULT]
            _process_payload(payload, obj)

        # in batch mode, we don't want a single error to cause the the entire batch
        # to retry. for that reason, we have opted to gobble all the errors here
        # and handle retries withing the fsm dispatch code.
        except Exception:
            logger.exception('Critical error handling record: %s', record)


def lambda_timer_handler():
    """
    AWS Lambda handler that runs periodically.
    """
    try:
        # TODO: hide these details behind an interface
        # TODO: handle missing dynamodb tables
        retries_table_arn = get_primary_retry_source()
        retries_table = get_arn_from_arn_string(retries_table_arn).resource.split('/')[-1]
        index = 'retries'

        # get this table name elsewhere...
        query = retriable_entities(
            retries_table,
            index,
            time.time()
        )
        entities = list(query)

    except Exception:  # pragma: no cover
        # logger.exception('Error querying retry entities.')
        entities = []

    if entities:
        logger.info('Processing %d entities from dynamodb retries...', len(entities))

    for entity in entities:

        try:
            obj = {OBJ.SOURCE: AWS.DYNAMODB_RETRY}
            payload = entity[RECOVERY_DATA.PAYLOAD]
            _process_payload(payload, obj)

        # see comment in lambda_kinesis_handler
        except Exception:
            logger.exception('Critical error handling entity: %s', entity)


def lambda_handler(lambda_event, lambda_context):
    """
    AWS Lambda handler that handles all Kinesis/DynamoDB/Timer/SNS events.
    """

    # {
    #   "account": "123456789012",
    #   "region": "us-east-1",
    #   "detail": {},
    #   "detail-type": "Scheduled Event",
    #   "source": "aws.events",
    #   "time": "1970-01-01T00:00:00Z",
    #   "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
    #   "resources": [
    #     "arn:aws:events:us-east-1:123456789012:rule/my-schedule"
    #   ]
    # }
    if 'source' in lambda_event and lambda_event['source'] == 'aws.events':
        lambda_timer_handler()

    # {
    #   "Records": [
    #     {
    #       "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
    #       "eventVersion": "1.0",
    #       "kinesis": {
    #         "partitionKey": "partitionKey-3",
    #         "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=",
    #         "kinesisSchemaVersion": "1.0",
    #         "sequenceNumber": "49545115243490985018280067714973144582180062593244200961"
    #       },
    #       "invokeIdentityArn": "arn:aws:iam::EXAMPLE",
    #       "eventName": "aws:kinesis:record",
    #       "eventSourceARN": "arn:aws:kinesis:EXAMPLE",
    #       "eventSource": "aws:kinesis",
    #       "awsRegion": "us-east-1"
    #     }
    #   ]
    # }
    elif 'Records' in lambda_event and lambda_event['Records'] and 'kinesis' in lambda_event['Records'][0]:
        lambda_kinesis_handler(lambda_event)

    # {
    #   "Records": [
    #     {
    #       "eventID": "1",
    #       "eventVersion": "1.0",
    #       "dynamodb": {
    #         "Keys": {
    #           "Id": {
    #             "N": "101"
    #           }
    #         },
    #         "NewImage": {
    #           "Message": {
    #             "S": "New item!"
    #           },
    #           "Id": {
    #             "N": "101"
    #           }
    #         },
    #         "StreamViewType": "NEW_AND_OLD_IMAGES",
    #         "SequenceNumber": "111",
    #         "SizeBytes": 26
    #       },
    #       "awsRegion": "us-west-2",
    #       "eventName": "INSERT",
    #       "eventSourceARN": "arn:aws:dynamodb:us-west-2:account-id:table/Example/stream/2015-06-27T00:48:05.899",
    #       "eventSource": "aws:dynamodb"
    #     }
    #   ]
    # }
    elif 'Records' in lambda_event and lambda_event['Records'] and 'dynamodb' in lambda_event['Records'][0]:
        lambda_dynamodb_handler(lambda_event)

    # {
    #   "Records": [
    #     {
    #       "EventVersion": "1.0",
    #       "EventSubscriptionArn": "arn:aws:sns:EXAMPLE",
    #       "EventSource": "aws:sns",
    #       "Sns": {
    #         "SignatureVersion": "1",
    #         "Timestamp": "1970-01-01T00:00:00.000Z",
    #         "Signature": "EXAMPLE",
    #         "SigningCertUrl": "EXAMPLE",
    #         "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
    #         "Message": u'{"default": "Hello from SNS!"}',
    #         "MessageAttributes": {
    #           "Test": {
    #             "Type": "String",
    #             "Value": "TestString"
    #           },
    #           "TestBinary": {
    #             "Type": "Binary",
    #             "Value": "TestBinary"
    #           }
    #         },
    #         "Type": "Notification",
    #         "UnsubscribeUrl": "EXAMPLE",
    #         "TopicArn": "arn:aws:sns:EXAMPLE",
    #         "Subject": "TestInvoke"
    #       }
    #     }
    #   ]
    # }
    elif 'Records' in lambda_event and lambda_event['Records'] and 'Sns' in lambda_event['Records'][0]:
        lambda_sns_handler(lambda_event)

    # API Gateway
    else:
        lambda_api_handler(lambda_event)
