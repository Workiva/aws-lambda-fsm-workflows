<!--
Copyright 2016-2020 Workiva Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

[<< Running on AWS](AWS.md) | [TODO: >>](TODO.md)

# Configuring Services

Each of the AWS services requires some initial setup and configuration. This is accomplished with 
python applications in the `tools` folder. These can be used to setup the local mock services, or
the actual AWS services (with valid `~/.aws/credentials`).

## Running `create_sqs_queue.py`

If any of `settings.PRIMARY|SECONDARY_STREAM_SOURCE` or `settings.PRIMARY|SECONDARY_RETRY_SOURCE`
are set to an [AWS SQS](https://aws.amazon.com/sqs/) ARN, then a queue must be created for each ARN. 
 
This creates a [AWS SQS](https://aws.amazon.com/sqs/) queue to publish fsm/event information.
 
```bash
$ workon aws-lambda-fsm
$ python tools/create_sqs_queue.py --sqs_queue_arn=PRIMARY_STREAM_SOURCE
```

## Running `create_kinesis_stream.py`

If any of `settings.PRIMARY|SECONDARY_STREAM_SOURCE` or `settings.PRIMARY|SECONDARY_RETRY_SOURCE`
are set to an [AWS Kinesis](https://aws.amazon.com/kinesis/) ARN, then a stream must be
created for each ARN. 
 
This creates a [AWS Kinesis](https://aws.amazon.com/kinesis/) stream to store fsm/event information.
 
```bash
$ workon aws-lambda-fsm
$ python tools/create_kinesis_stream.py --kinesis_stream_arn=PRIMARY_STREAM_SOURCE
```
    
## Running `create_sns_topic.py`

If any of `settings.PRIMARY|SECONDARY_STREAM_SOURCE` or `settings.PRIMARY|SECONDARY_RETRY_SOURCE`
are set to an [AWS SNS](https://aws.amazon.com/sns/) ARN, then a topic must be
created for each ARN. 
 
This creates an SNS topic to store fsm/event information.
 
```bash
$ workon aws-lambda-fsm
$ python tools/create_sns_topic.py --sns_topic_arn=PRIMARY_STREAM_SOURCE
```
    
## Running `create_dynamodb_table.py`

If any of `settings.PRIMARY|SECONDARY_RETRY_SOURCE`, `settings.PRIMARY|SECONDARY_CHECKPOINT_SOURCE`,
`settings.PRIMARY|SECONDARY_ENVIRONMENT_SOURCE` or`settings.PRIMARY|SECONDARY_STREAM_SOURCE` 
are set to an [AWS DynamoDB](https://aws.amazon.com/dynamodb/) ARN, then a table must be
created for each ARN. 
 
This creates a number of DynamoDB tables to store checkpoint and retry information.
 
 ```bash
$ workon aws-lambda-fsm
$ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_CHECKPOINT_SOURCE
$ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_RETRY_SOURCE
$ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_ENVIRONMENT_SOURCE
$ python tools/create_dynamodb_table.py --dynamodb_table_arn=SECONDARY_STREAM_SOURCE
```
    
The strings `PRIMARY_CHECKPOINT_SOURCE` etc. are obtained from you current `settings.py`/`settingslocal.py`.
If the setting for `SECONDARY_STREAM_SOURCE` for example, is not a `dynamodb` ARN, then the script will
warn the user and do nothing. All scripts have similar validation.
    
[<< Running on AWS](AWS.md) | [TODO: >>](TODO.md)
