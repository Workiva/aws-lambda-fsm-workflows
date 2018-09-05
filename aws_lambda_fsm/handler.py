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
import base64
import json
import logging
import time
from collections import Counter

# application imports
from aws_lambda_fsm.fsm import Context
from aws_lambda_fsm.fsm import FSM  # noqa
from aws_lambda_fsm.aws import retriable_entities
from aws_lambda_fsm.aws import get_primary_retry_source
from aws_lambda_fsm.aws import validate_config
from aws_lambda_fsm.constants import OBJ
from aws_lambda_fsm.constants import STATE
from aws_lambda_fsm.constants import AWS_LAMBDA
from aws_lambda_fsm.constants import SYSTEM_CONTEXT
from aws_lambda_fsm.constants import RETRY_DATA
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import STREAM_DATA
from aws_lambda_fsm.constants import AWS_DYNAMODB
from aws_lambda_fsm.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

validate_config()


def _process_payload(payload_str, obj):
    """
    Internal function to turn a json fsm payload (from an AWS Lambda event),
    into an fsm Context, and then dispatch the event and execute user code.

    :param payload_str: a json string like '{"serialized": "data"}'
    :param obj: a dict to pass to fsm Context.dispatch(...)
    """
    payload = json.loads(payload_str)
    obj[OBJ.PAYLOAD] = payload_str
    fsm = Context.from_payload_dict(payload)
    logger.info('system_context=%s', fsm.system_context())
    logger.info('user_context.keys()=%s', fsm.user_context().keys())
    current_event = fsm.system_context().get(SYSTEM_CONTEXT.CURRENT_EVENT, STATE.PSEUDO_INIT)
    fsm.dispatch(current_event, obj)


def _process_payload_step(payload_str, obj):
    """
    Internal function to turn a json fsm payload (from an AWS Lambda event),
    into an fsm Context, and then dispatch the event and execute user code.

    This function is ONLY used in the AWS Step Function execution path.

    :param payload_str: a json string like '{"serialized": "data"}'
    :param obj: a dict to pass to fsm Context.dispatch(...)
    """
    payload = json.loads(payload_str)
    obj[OBJ.PAYLOAD] = payload_str
    fsm = Context.from_payload_dict(payload)
    logger.info('system_context=%s', fsm.system_context())
    logger.info('user_context.keys()=%s', fsm.user_context().keys())
    current_event = fsm.system_context().get(SYSTEM_CONTEXT.CURRENT_EVENT, STATE.PSEUDO_INIT)

    # all retries etc. are handled by AWS Step Function infrastructure
    # so this an entirely stripped down dispatch running ONLY the user
    # Actions, and NONE of the framework's retry etc. code.
    next_event = fsm.current_state.dispatch(fsm, current_event, obj)
    if next_event:
        fsm.current_event = next_event
        data = fsm.to_payload_dict()
        data[AWS.STEP_FUNCTION] = True
        return data


def lambda_api_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event: a dict event from AWS Lambda
    """
    try:
        obj = {OBJ.SOURCE: AWS.GATEWAY}
        payload = json.dumps(lambda_event)  # API Gateway just passes straight though
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        lambda_event = AWS_LAMBDA.REDACTED
        logger.exception('Critical error handling lambda: %s', lambda_event)


def lambda_step_handler(lambda_event):
    """
    AWS Lambda handler for executing state machines.

    :param lambda_event: a dict event from AWS Lambda
    :return: a dict event to pass along to AWS Step Functions orchestration
    """
    obj = {OBJ.SOURCE: AWS.STEP_FUNCTION}
    payload = json.dumps(lambda_event)  # Step Function just passes straight though
    return _process_payload_step(payload, obj)


def lambda_sqs_handler(record):
    """
    AWS Lambda handler for executing state machines.

    :param record: a dict event from AWS Lambda
    """

    try:
        obj = {OBJ.SOURCE: AWS.SQS, OBJ.LAMBDA_RECORD: record}
        payload = record[AWS_LAMBDA.SQS_RECORD.BODY]
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        record[AWS_LAMBDA.SQS_RECORD.BODY] = AWS_LAMBDA.REDACTED
        logger.exception('Critical error handling record: %s', record)


def lambda_kinesis_handler(record):
    """
    AWS Lambda handler for executing state machines.

    :param record: a dict event from AWS Lambda
    """

    try:
        obj = {OBJ.SOURCE: AWS.KINESIS, OBJ.LAMBDA_RECORD: record}
        kinesis = record[AWS_LAMBDA.KINESIS_RECORD.KINESIS]
        encoded = kinesis[AWS_LAMBDA.KINESIS_RECORD.DATA]
        payload = base64.b64decode(encoded)
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        record.get(AWS_LAMBDA.KINESIS_RECORD.KINESIS, {})[AWS_LAMBDA.KINESIS_RECORD.DATA] = AWS_LAMBDA.REDACTED
        logger.exception('Critical error handling record: %s', record)


def lambda_dynamodb_handler(record):
    """
    AWS Lambda handler for executing state machines.

    :param record: a dict event from AWS Lambda
    """

    try:
        obj = {OBJ.SOURCE: AWS.DYNAMODB_STREAM, OBJ.LAMBDA_RECORD: record}
        dynamodb = record[AWS_LAMBDA.DYNAMODB_RECORD.DYNAMODB]
        new_image = dynamodb[AWS_LAMBDA.DYNAMODB_RECORD.NewImage]
        payload = new_image[STREAM_DATA.PAYLOAD][AWS_DYNAMODB.STRING]
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        record.get(AWS_LAMBDA.DYNAMODB_RECORD.DYNAMODB, {}) \
            .get(AWS_LAMBDA.DYNAMODB_RECORD.NewImage, {}) \
            .get(STREAM_DATA.PAYLOAD, {})[AWS_DYNAMODB.STRING] = AWS_LAMBDA.REDACTED
        logger.exception('Critical error handling record: %s', record)


def lambda_sns_handler(record):
    """
    AWS Lambda handler for executing state machines.

    :param record: a dict event from AWS Lambda
    """

    try:
        obj = {OBJ.SOURCE: AWS.SNS, OBJ.LAMBDA_RECORD: record}
        sns = record[AWS_LAMBDA.SNS_RECORD.SNS]
        payload = sns[AWS_LAMBDA.SNS_RECORD.Message]
        _process_payload(payload, obj)

    # in batch mode, we don't want a single error to cause the the entire batch
    # to retry. for that reason, we have opted to gobble all the errors here
    # and handle retries withing the fsm dispatch code.
    except Exception:
        record.get(AWS_LAMBDA.SNS_RECORD.SNS, {})[AWS_LAMBDA.SNS_RECORD.Message] = AWS_LAMBDA.REDACTED
        logger.exception('Critical error handling record: %s', record)


def lambda_timer_handler():
    """
    AWS Lambda handler that runs periodically.
    """
    try:
        # TODO: hide these details behind an interface
        # TODO: handle missing dynamodb tables
        retries_table_arn = get_primary_retry_source()
        index = 'retries'

        # get this table name elsewhere...
        query = retriable_entities(
            retries_table_arn,
            index,
            time.time()
        )
        entities = list(query)

    except Exception:  # pragma: no cover
        logger.exception('Error querying retry entities.')
        entities = []

    if entities:
        logger.info('Processing %d entities from dynamodb retries...', len(entities))

    for entity in entities:

        try:
            obj = {OBJ.SOURCE: AWS.DYNAMODB_RETRY}
            payload = entity[RETRY_DATA.PAYLOAD]
            _process_payload(payload, obj)

        # see comment in lambda_kinesis_handler
        except Exception:
            logger.exception('Critical error handling entity: %s', entity)


def _get_event_source(record):
    return record.get(AWS_LAMBDA.EventSource, record.get(AWS_LAMBDA.EventSourceCaps))


def lambda_handler(lambda_event, lambda_context):
    """
    AWS Lambda handler that handles all Kinesis/DynamoDB/Timer/SNS/SQS events.
    """

    # https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-scheduled-event
    #
    # {
    #     "account": "123456789012",
    #     "region": "us-east-1",
    #     "detail": {},
    #     "detail-type": "Scheduled Event",
    #     "source": "aws.events",
    #     "time": "1970-01-01T00:00:00Z",
    #     "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
    #     "resources": [
    #         "arn:aws:events:us-east-1:123456789012:rule/my-schedule"
    #     ]
    # }
    if lambda_event.get(AWS_LAMBDA.Source) == AWS_LAMBDA.SOURCE.EVENTS:
        lambda_timer_handler()

    elif AWS_LAMBDA.Records in lambda_event:

        records = lambda_event.get(AWS_LAMBDA.Records, [])

        logger.info('Processing %s records...', Counter(_get_event_source(record) for record in records))

        for record in records:

            # https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-kinesis-streams
            #
            # {
            #     "Records": [
            #         {
            #             "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
            #             "eventVersion": "1.0",
            #             "kinesis": {
            #                 "partitionKey": "partitionKey-3",
            #                 "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=",
            #                 "kinesisSchemaVersion": "1.0",
            #                 "sequenceNumber": "49545115243490985018280067714973144582180062593244200961"
            #             },
            #             "invokeIdentityArn": identityarn,
            #             "eventName": "aws:kinesis:record",
            #             "eventSourceARN": eventsourcearn,
            #             "eventSource": "aws:kinesis",
            #             "awsRegion": "us-east-1"
            #         }, ...
            #     ]
            # }
            if record.get(AWS_LAMBDA.EventSource) == AWS_LAMBDA.EVENT_SOURCE.KINESIS:
                lambda_kinesis_handler(record)

            # https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-ddb-update
            #
            # {
            #     "Records": [
            #         {
            #             "eventID": "1",
            #             "eventVersion": "1.0",
            #             "dynamodb": {
            #                 "Keys": {
            #                     "Id": {
            #                         "N": "101"
            #                     }
            #                 },
            #                 "NewImage": {
            #                     "Message": {
            #                         "S": "New item!"
            #                     },
            #                     "Id": {
            #                         "N": "101"
            #                     }
            #                 },
            #                 "StreamViewType": "NEW_AND_OLD_IMAGES",
            #                 "SequenceNumber": "111",
            #                 "SizeBytes": 26
            #             },
            #             "awsRegion": "us-west-2",
            #             "eventName": "INSERT",
            #             "eventSourceARN": eventsourcearn,
            #             "eventSource": "aws:dynamodb"
            #         }, ...
            #     ]
            # }
            elif record.get(AWS_LAMBDA.EventSource) == AWS_LAMBDA.EVENT_SOURCE.DYNAMODB:
                lambda_dynamodb_handler(record)

            # https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sns
            #
            # {
            #     "Records": [
            #         {
            #             "EventVersion": "1.0",
            #             "EventSubscriptionArn": eventsubscriptionarn,
            #             "EventSource": "aws:sns",
            #             "Sns": {
            #                 "SignatureVersion": "1",
            #                 "Timestamp": "1970-01-01T00:00:00.000Z",
            #                 "Signature": "EXAMPLE",
            #                 "SigningCertUrl": "EXAMPLE",
            #                 "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
            #                 "Message": "Hello from SNS!",
            #                 "MessageAttributes": {
            #                     "Test": {
            #                         "Type": "String",
            #                         "Value": "TestString"
            #                     },
            #                     "TestBinary": {
            #                         "Type": "Binary",
            #                         "Value": "TestBinary"
            #                     }
            #                 },
            #                 "Type": "Notification",
            #                 "UnsubscribeUrl": "EXAMPLE",
            #                 "TopicArn": topicarn,
            #                 "Subject": "TestInvoke"
            #             }
            #         }, ...
            #     ]
            # }
            elif record.get(AWS_LAMBDA.EventSource) == AWS_LAMBDA.EVENT_SOURCE.SNS:
                lambda_sns_handler(record)

            # https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sqs
            #
            # {
            #     "Records": [
            #         {
            #             "messageId": "c80e8021-a70a-42c7-a470-796e1186f753",
            #             "receiptHandle": "AQEBJQ+/u6NsnT5t8Q/VbVxgVP...GwvTQ==",
            #             "body": "{\"foo\":\"bar\"}",
            #             "attributes": {
            #                 "ApproximateReceiveCount": "3",
            #                 "SentTimestamp": "1529104986221",
            #                 "SenderId": "594035263019",
            #                 "ApproximateFirstReceiveTimestamp": "1529104986230"
            #             },
            #             "messageAttributes": {},
            #             "md5OfBody": "9bb58f26192e4ba00f01e2e7b136bbd8",
            #             "eventSource": "aws:sqs",
            #             "eventSourceARN": "arn:aws:sqs:us-west-2:594035263019:NOTFIFOQUEUE",
            #             "awsRegion": "us-west-2"
            #         }, ...
            #     ]
            # }
            elif record.get(AWS_LAMBDA.EventSource) == AWS_LAMBDA.EVENT_SOURCE.SQS:
                lambda_sqs_handler(record)

    # TODO: see if there is some other way to distinguish step function calls from api gateway calls
    #       injecting a parameter is not ideal

    # Step Functions
    elif lambda_event.get(AWS.STEP_FUNCTION):
        return lambda_step_handler(lambda_event)

    # API Gateway
    else:
        lambda_api_handler(lambda_event)
