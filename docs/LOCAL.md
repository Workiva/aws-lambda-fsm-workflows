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

[<< FSM YAML](YAML.md) | [Running on AWS >>](AWS.md)

# Running Locally

## 1. Install Docker

Install [Docker](https://www.docker.com/).

## 2. Install Virtualenv

Installing [Virtualenv](https://virtualenv.pypa.io/en/stable/) and 
[Virtualenv Wrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) allows all the python dependencies to be 
installed locally, so that state machines can be run and debugged locally using `dev_lambda.py` (see below). 

```bash
$ pip install virtualenv
$ pip install virtualenvwrapper
$ mkvirtualenv aws-lambda-fsm
$ pip install -Ur requirements_dev.txt
```

## 3. Localstack + Memcache + Redis

A `docker-compose.yaml` file is supplied that runs [LocalStack](https://github.com/localstack/localstack), which 
contains stubs for AWS services like SQS, Kinesis, etc. 

It also runs [Memcached](https://memcached.org/) and [Redis](https://redis.io/) instances, since 
[AWS Elasticache](https://aws.amazon.com/elasticache/) is not supported by 
[LocalStack](https://github.com/localstack/localstack).

```bash
$ docker-compose -f tools/experimental/docker-compose.yaml up localstack memcached redis
```

## 4. Settings

See [Settings](SETTINGS.md)

It is possible to use local stubs for various AWS services. An example that can be used for
local development is included in the `tools/experimental` folder. You can simply symlink it as 
follows.

```bash
$ ln -s tools/experimental/settings.py.dev_ecs settings.py
```

It contains a mapping of ARN to localstack/memcache endpoint

```python
ENDPOINTS = {
     'arn:partition:dynamodb:testing:account:table/aws-lambda-fsm.env': 'http://%s:4569' % os.environ.get('DYNAMODB_HOST', 'localstack'),
     'arn:partition:kinesis:testing:account:stream/aws-lambda-fsm': 'http://%s:4568' % os.environ.get('KINESIS_HOST', 'localstack'),
     'arn:partition:elasticache:testing:account:cluster:aws-lambda-fsm': '%s:11211' % os.environ.get('MEMCACHE_HOST', 'memcached'),
     'arn:aws:ecs:testing:account:cluster/aws-lambda-fsm': 'http://%s:8888' % os.environ.get('ECS_HOST', 'ecs')
}
```
    
## 5. Running `create_resources.py`

See [Setup](SETUP.md)

You can setup all the resource in `settings(local).py` using the `create_resources.py` script. Feel free
to `export` to the environment variables rather than adding them to every comment. They are only listed
to be explicit about requirements.

```bash
$ workon aws-lambda-fsm
$ PATH=tools:${PATH} PYTHONPATH=. DYNAMODB_HOST=localhost KINESIS_HOST=localhost MEMCACHE_HOST=localhost ECS_HOST=localhost \
  python tools/create_resources.py
...
INFO:root:********************************************************************************
INFO:root:CREATING arn:partition:dynamodb:testing:account:table/aws-lambda-fsm.env
INFO:root:********************************************************************************
...
```

## 6. Running `dev_lambda.py`

This runs a local [AWS Lambda](https://aws.amazon.com/lambda/)-like service. In the following example
we have started up a `dev_lambda.py` listening to the default `settings.PRIMARY_STREAM_SOURCE`. It is
possible to start multiple `dev_lambda.py` instances, each listening to different event sources, 
as per the configuration in your settings.

```bash
$ workon aws-lambda-fsm
$ AWS_DEFAULT_REGION=testing PYTHONPATH=. DYNAMODB_HOST=localhost KINESIS_HOST=localhost MEMCACHE_HOST=localhost ECS_HOST=localhost \
  python tools/dev_lambda.py --run_kinesis_lambda=1
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
```
   
## 7. Running `start_state_machine.py`

This injects a message into [AWS Kinesis](https://aws.amazon.com/kinesis/), 
[AWS DynamoDB](https://aws.amazon.com/dynamodb/), [AWS SNS](https://aws.amazon.com/sns/), or
[AWS SQS](https://aws.amazon.com/sqs/) to kick off a state machine.
 
```bash
$ workon aws-lambda-fsm
$ PYTHONPATH=. DYNAMODB_HOST=localhost KINESIS_HOST=localhost MEMCACHE_HOST=localhost ECS_HOST=localhost \
  python tools/start_state_machine.py --machine_name=tracer
```
 
## 8. (EXPERIMENTAL) Running `dev_ecs.py`

This runs a local [AWS ECS](https://aws.amazon.com/ecs/)-like service.

Build the `docker` image that knows how to run other `docker` images and emit
events back onto the FSM's event dispatching machinery.

```bash
$ docker build -t fsm_docker_runner -f ./tools/experimental/Dockerfile.fsm_docker_runner ./tools/experimental
$ docker build -t dev_ecs -f ./tools/experimental/Dockerfile.dev_ecs ./tools/experimental
```
 
`dev_ecs.py` is configured to run in the supplied `docker-compose.yaml`. Just start it up alongside
[LocalStack](https://github.com/localstack/localstack) etc. using

```bash
$ docker-compose -f tools/experimental/docker-compose.yaml up
```

It can also be run directly in a terminal like `dev_lambda.py` above.
    
[<< FSM YAML](YAML.md) | [Running on AWS >>](AWS.md)
    
