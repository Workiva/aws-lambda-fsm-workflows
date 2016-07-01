# Running Locally

## Settings

It is possible to use local stubs for various AWS services. By adding the following
to `settingslocal.py` the framework will send requests to localhost, rather than 
amazonaws.com.

    ENDPOINTS = {
        'kinesis': {
            'us-east-1': 'http://localhost:4567'
        },
        'dynamodb': {
            'us-east-1': 'http://localhost:7654'
        },
        'elasticache': {
            'us-east-1': 'localhost:11211'
        },
        'sns': {
            'us-east-1': 'http://localhost:9292'
        },
        'ecs': {
            'us-east-1': 'http://localhost:8888'
        }
    }

## Running Services

Depending on the configuration in `settings.py`, the following local services should be started
    
### Running `kinesalite` (https://github.com/mhart/kinesalite)

    $ kinesalite --port 4567
    Listening at http://:::4567
    
### Running `dynalite` (https://github.com/mhart/dynalite)

    $ dynalite --port 7654
    Listening at http://:::7654
    
### Running `memcached` (http://memcached.org/downloads)

    $ memcached -vv
    slab class   1: chunk size        96 perslab   10922
    ...
    
### Running `fake_sns` (https://github.com/yourkarma/fake_sns)

    $ fake_sns --database :memory:
    
`fake_sns` does not automatically send the messages, so one needs to repeatedly use `curl`
to step the state machines forward.

    $ curl -X POST http://localhost:9292/drain # drain the messages

### Running `fake_sqs` (https://github.com/iain/fake_sqs)

    $ fake_sqs
    
### Running `dev_ecs` 

Get host etc to docker daemon setup

    $ export DOCKER_HOST=tcp://192.168.99.100:2376
    $ export DOCKER_MACHINE_NAME=default
    $ export DOCKER_TLS_VERIFY=1
    $ export DOCKER_CERT_PATH=/Users/shawnrusaw/.docker/machine/machines/default

Run `dev_ecs.py` which uses `docker run` to simulate an AWS ECS service

    $ workon aws-lambda-fsm
    $ python tools/dev_ecs.py --port=8888 --image=image:tag
    
### Running `dev_lambda.py`

This runs a local Lambda service.
 
    $ workon aws-lambda-fsm
    $ python tools/dev_lambda.py --kinesis_uri=http://localhost:4567 --dynamodb_uri=http://localhost:7654 --memcache_uri=localhost:11211
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
    
### Running `start_state_machine.py`

This injects a message into Kinesis/DynamoDB/SNS to kick off a state machine.
 
    $ workon aws-lambda-fsm
    $ python tools/start_state_machine.py --machine_name=tracer --kinesis_uri=http://localhost:4567 --dynamodb_uri=http://localhost:7654

    
