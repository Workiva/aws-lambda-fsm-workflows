# system imports
import os
import time
import logging
import json

# library imports
import boto3

# application imports
from aws_lambda_fsm.constants import AWS_SQS
from aws_lambda_fsm.constants import AWS_KINESIS
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import PAYLOAD
from aws_lambda_fsm.constants import SYSTEM_CONTEXT
from aws_lambda_fsm.aws import get_arn_from_arn_string

# TODO: local cache of processed message ids
# TODO: handle partial batch kinesis failure
# TODO: handle partial sns failure
# TODO: handle partial batch sqs failure
# TODO: start via supervisord

logging.getLogger().setLevel(logging.INFO)

sqs_arn_string = os.environ['SQS_ARN']
dest_arn_string = os.environ['DEST_ARN']

logging.info('SQS_ARN = %s', sqs_arn_string)
logging.info('DEST_ARN = %s', dest_arn_string)

sqs_arn = get_arn_from_arn_string(sqs_arn_string)
dest_arn = get_arn_from_arn_string(dest_arn_string)

sqs = boto3.client(sqs_arn.service, region_name=sqs_arn.region_name)
dest = boto3.client(dest_arn.service, region_name=sqs_arn.region_name)

wait = 5
backoff = 0

response = sqs.get_queue_url(
    QueueName=sqs_arn.resource.split(':')[-1]
)
sqs_queue_url = response[AWS_SQS.QueueUrl]

logging.info('sqs queue url = %s', sqs_queue_url)

# this service will run forever, echoing messages from SQS
# onto another Amazon service.
while True:

    try:
        # set the backoff in case of error
        backoff += 5

        # receive up to 10 messages from SQS
        sqs_messages = []
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10
        )
        sqs_messages = response.get(AWS_SQS.Messages, [])

        # if not SQS messages were received, then we simply wait a little bit
        # and try again. we wait to avoid hitting the SQS endpoint too often.
        if not sqs_messages:
            logging.info('No messages. Sleeping for %d seconds', wait)
            time.sleep(wait)
            continue

        # now echo them one at a time to SNS
        logging.info('Echoing %d messages from %s to %s...',
                     len(sqs_messages),
                     sqs_arn_string,
                     dest_arn_string)

        # echo to SNS
        if dest_arn.service == AWS.SNS:
            for sqs_message in sqs_messages:
                response = dest.publish(
                    TopicArn=dest_arn,
                    Message=sqs_message[AWS_SQS.MESSAGE.Body],
                )
                logging.info(response)

        # echo to Kinesis
        elif dest_arn.service == AWS.KINESIS:
            records = []
            for sqs_message in sqs_messages:
                body = sqs_message[AWS_SQS.MESSAGE.Body]
                payload = json.loads(body)
                system_context = payload[PAYLOAD.SYSTEM_CONTEXT]
                correlation_id = system_context[SYSTEM_CONTEXT.CORRELATION_ID]
                response = records.append(
                    {
                        AWS_KINESIS.RECORD.Data: body,
                        AWS_KINESIS.RECORD.PartitionKey: correlation_id
                    }
                )
            response = dest.put_records(
                StreamName=dest_arn.resource.split('/')[-1],
                Records=records
            )
            logging.info(response)

        # after processing, the SQS messages need to be deleted
        sqs.delete_message_batch(
            QueueUrl=sqs_queue_url,
            Entries=[
                {
                    AWS_SQS.MESSAGE.Id: str(i),
                    AWS_SQS.MESSAGE.ReceiptHandle: sqs_message[AWS_SQS.MESSAGE.ReceiptHandle]
                }
                for i, sqs_message in enumerate(sqs_messages)
            ]
        )
        backoff = 0

    except Exception:

        logging.exception('Exception occurred. Sleeping for %d seconds', backoff)
        time.sleep(backoff)
