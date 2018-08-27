<!--
Copyright 2016-2018 Workiva Inc.

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

[<< FSM YAML](YAML.md) | [Running on AWS >>](AWS.md)

# Running Locally

## Install Virtualenv

    $ pip install virtualenv
    $ pip install virtualenvwrapper
    
## Install Python Dependencies

    $ mkvirtualenv aws-lambda-fsm
    $ pip install -Ur requirements_dev.txt

## Install Docker

Install `docker` (https://www.docker.com/)

## Localstack + Memcache + Redis

A docker-compose.yaml file is supplied that runs localstack (https://github.com/localstack/localstack), which contains 
stubs for AWS services like SQS, Kinesis, etc. It also runs memcache and redis instances, since 
[Elasticache](https://aws.amazon.com/elasticache/) is not supported by localstack.

    $ docker-compose -f tools/docker-compose.yaml up

## Settings

It is possible to use local stubs for various AWS services. An example that can be used local
development is included in the `tools` folder. You can simply symlink it as follows.

    $ ln -s tools/settings.py.local settings.py

It contains a mapping of ARN to localstack/memcache endpoint

    ENDPOINTS = {
        'arn:partition:kinesis:testing:account:stream/aws-lambda-fsm': 'http://localhost:4568',
        'arn:partition:elasticache:testing:account:cluster:aws-lambda-fsm': 'localhost:11211'
    }

## Running `dev_lambda.py`

This runs a local Lambda service.
 
    $ workon aws-lambda-fsm
    $ ln -s tools/settings.py.local settings.py
    $ PYTHONPATH=. python tools/dev_lambda.py --kinesis_uri=http://localhost:4567 --dynamodb_uri=http://localhost:7654 --memcache_uri=localhost:11211
    INFO:root:fsm data={u'current_state': u's1', u'current_event': u'e1', u'machine_name': u'm1'}
    INFO:root:action.name=s1-exit-action
    INFO:root:action.name=t1-action
    INFO:root:action.name=s2-entry-action
    INFO:root:action.name=a5-do-action
    INFO:root:fsm data={u'current_state': u's2', u'current_event': u'e2', u'machine_name': u'm1', u'context': {}}
    INFO:root:action.name=s2-exit-action
    INFO:root:action.name=t2-action
    INFO:root:action.name=s1-entry-action
    ...
    
## Running `dev_ecs.py`

This runs a local ECS-like service.
 
Get host etc to docker daemon setup

    $ export DOCKER_HOST=tcp://192.168.99.100:2376
    $ export DOCKER_MACHINE_NAME=default
    $ export DOCKER_TLS_VERIFY=1
    $ export DOCKER_CERT_PATH=/absolute/path/to/.docker/machine/machines/default
    
Build the `docker` image that knows how to run other `docker` images and emit
events back onto the FSM's Kinesis/SQS/... stream/queue/...

    $ docker build -t runner tools
 
Run `dev_ecs.py` which uses `docker run` to simulate an AWS ECS service

    $ workon aws-lambda-fsm
    $ PYTHONPATH=. python tools/dev_ecs.py --port=8888 --image=runner:latest
    
## Running `start_state_machine.py`

This injects a message into Kinesis/DynamoDB/SNS to kick off a state machine.
 
    $ workon aws-lambda-fsm
    $ PYTHONPATH=. python tools/start_state_machine.py --machine_name=tracer --kinesis_uri=http://localhost:4567 --dynamodb_uri=http://localhost:4568

[<< FSM YAML](YAML.md) | [Running on AWS >>](AWS.md)
    
