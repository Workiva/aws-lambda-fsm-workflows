[//]: # (Copyright 2016 Workiva Inc.)
[//]: # ()
[//]: # (Licensed under the Apache License, Version 2.0 (the "License");)
[//]: # (you may not use this file except in compliance with the License.)
[//]: # (You may obtain a copy of the License at)
[//]: # ()
[//]: # (http://www.apache.org/licenses/LICENSE-2.0)
[//]: # ()
[//]: # (Unless required by applicable law or agreed to in writing, software)
[//]: # (distributed under the License is distributed on an "AS IS" BASIS,)
[//]: # (WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.)
[//]: # (See the License for the specific language governing permissions and)
[//]: # (limitations under the License.)

# Configuring Services

Each of the AWS services requires some initial setup and configuration. This is accomplished with 
python applications in the `tools` folder. These can be used to setup the local mock services, or
the actual AWS services (with valid `~/.aws/credentials`).
    
## Running `create_dynamodb_table.py`
 
This creates a number of DynamoDB tables to store checkpoint and retry information.
 
    $ workon aws-lambda-fsm
    $ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_CHECKPOINT_SOURCE
    $ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_RETRY_SOURCE
    $ python tools/create_dynamodb_table.py --dynamodb_table_arn=PRIMARY_ENVIRONMENT_SOURCE
    $ python tools/create_dynamodb_table.py --dynamodb_table_arn=SECONDARY_STREAM_SOURCE
    
The strings `PRIMARY_CHECKPOINT_SOURCE` etc. are obtained from you current `settings.py`/`settingslocal.py`.
If the setting for `SECONDARY_STREAM_SOURCE` for example, is not a `dynamodb` ARN, then the script will
warn the user and do nothing. All scripts have similar validation.
    
## Running `create_kinesis_stream.py`
 
This creates a Kinesis stream to store fsm/event information.
 
    $ workon aws-lambda-fsm
    $ python tools/create_kinesis_stream.py --kinesis_stream_arn=PRIMARY_STREAM_SOURCE
    
## Running `create_sns_topic.py`
 
This creates an SNS topic to store fsm/event information.
 
    $ workon aws-lambda-fsm
    $ python tools/create_sns_topic.py --sns_topic_arn=PRIMARY_STREAM_SOURCE
    