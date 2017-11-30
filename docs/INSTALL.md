<!--
Copyright 2016-2017 Workiva Inc.

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

[<< Justification](JUSTIFICATION.md) | [Settings >>](SETTINGS.md)

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

    $ cd /tmp
    $ git clone https://github.com/yourkarma/fake_sns
    $ cd fake_sns
    $ echo "diff --git a/lib/fake_sns/server.rb b/lib/fake_sns/server.rb
    index 6713789..071ec1d 100644
    --- a/lib/fake_sns/server.rb
    +++ b/lib/fake_sns/server.rb
    @@ -57,6 +57,7 @@ module FakeSNS
               database.each_deliverable_message do |subscription, message|
                 DeliverMessage.call(subscription: subscription, message: message, request: request, config: config)
               end
    +          database.messages.reset
             end
           rescue => e
             status 500" | patch -p1
    $ gem build fake_sns.gemspec
    $ gem install fake_sns*.gem
    
NOTE: this does not work properly, and required local edits. probably not worth the effort.

## fake_sqs (https://github.com/iain/fake_sqs)

Install `fake_sqs`, which acts like a local SQS service and makes local development much more friendly.

    $ cd /tmp
    $ git clone git@github.com:iain/fake_sqs.git
    $ cd fake_sqs
    $ echo "diff --git a/lib/fake_sqs/web_interface.rb b/lib/fake_sqs/web_interface.rb
    index 4d49c50..25e62eb 100644
    --- a/lib/fake_sqs/web_interface.rb
    +++ b/lib/fake_sqs/web_interface.rb
    @@ -32,7 +32,7 @@ module FakeSQS
         post "/" do
           params['logger'] = logger
           if params['QueueUrl']
    -        queue = URI.parse(params['QueueUrl']).path.gsub(/\//, '')
    +        queue = URI.parse(params['QueueUrl']).path.split(/\//)[-1]
             return settings.api.call(action, queue, params) unless queue.empty?
           end
    " | patch -p1
    $ gem build fake_sqs.gemspec
    $ gem install fake_sqs*.gem

## docker (https://www.docker.com/)

Install `docker`, which acts like a local ECS service in conjunction with [dev_ecs.py](tools/dev_ecs.py)

    $ ...

[<< Justification](JUSTIFICATION.md) | [Settings >>](SETTINGS.md)

