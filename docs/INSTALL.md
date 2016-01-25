# Install Dependencies

## Common Dependencies

These are the dependencies required for local dev and for running on AWS

### Virtualenv

    $ mkvirtualenv aws-lambda-fsm
    $ pip install -Ur requirements_dev.txt
    
## Local Dependencies
    
These are the dependencies required for local dev only

## kinesalite (https://github.com/mhart/kinesalite)

Install `kinesalite`, which acts like a local Kinesis service and makes local development much more friendly.

    $ npm install kinesalite
    
## dynalite (https://github.com/mhart/dynalite)

Install `dynalite`, which acts like a local DynamoDB service and makes local development much more friendly.

    $ npm install dynalite
    
## memcached (http://memcached.org/downloads)

Install `memcached`, which act like a local memcached service and makes local development much more friendly.

    $ ./configure ...

## fake_sns (https://github.com/yourkarma/fake_sns)

Install `fake_sns`, which acts like a local SNS service and makes local development much more friendly.

    $ gem install fake_sns
    
NOTE: this does not work properly, and required local edits. probably not worth the effort.

## docker (https://www.docker.com/)

Install `docker`, which acts like a local ECS service in conjunction with [dev_ecs.py](tools/dev_ecs.py)

    $ ...


