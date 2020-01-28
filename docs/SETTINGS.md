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

[<< Justification](JUSTIFICATION.md) | [Chaos >>](CHAOS.md)

# Settings

The behaviour of the framework is controlled by a few important settings

## Event Streaming

* `settings.PRIMARY_STREAM_SOURCE` controls the primary location for event messages. Valid values are AWS ARNs for [AWS Kinesis](https://aws.amazon.com/kinesis/), [AWS DynamoDB](https://aws.amazon.com/dynamodb/), [AWS SNS](https://aws.amazon.com/sns/) and [AWS SQS](https://aws.amazon.com/sqs/).
* `settings.SECONDARY_STREAM_SOURCE` controls the secondary/failover location for event messages. Valid values are AWS ARNs for [AWS Kinesis](https://aws.amazon.com/kinesis/), [AWS DynamoDB](https://aws.amazon.com/dynamodb/), [AWS SNS](https://aws.amazon.com/sns/) and [AWS SQS](https://aws.amazon.com/sqs/).

It is valid, but not useful, to specify the same value for each. 

AWS ARN for [AWS Kinesis](https://aws.amazon.com/kinesis/) is the preferred value since it 

1. keeps a record of the entire history of the state machine
2. allows a stalled state machine to be restored from a checkpoint and replayed
3. append-only logs and state machines are very, very related [logs](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) & [fsm](https://www.cs.cornell.edu/fbs/publications/smsurvey.pdf)

## Checkpointing

* `settings.PRIMARY_CHECKPOINT_SOURCE` controls the primary location for checkpoint messages. Valid values are AWS ARNs for [AWS DynamoDB](https://aws.amazon.com/dynamodb/).
* `settings.SECONDARY_CHECKPOINT_SOURCE` controls the secondary/failover location for checkpoint messages. Valid values are AWS ARNs for [AWS DynamoDB](https://aws.amazon.com/dynamodb/).

It is valid, but not useful, to specify the same value for each. 

AWS ARN for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) is the preferred value since it 

1. persists even when the state machine dies

## Retries

* `settings.PRIMARY_RETRY_SOURCE` controls the primary location for retry messages. Valid values are AWS ARNs for [AWS Kinesis](https://aws.amazon.com/kinesis/), [AWS DynamoDB](https://aws.amazon.com/dynamodb/), [AWS SNS](https://aws.amazon.com/sns/) and [AWS SQS](https://aws.amazon.com/sqs/).
* `settings.SECONDARY_RETRY_SOURCE` controls the secondary/failover location for retry messages. Valid values are AWS ARNs for [AWS Kinesis](https://aws.amazon.com/kinesis/), [AWS DynamoDB](https://aws.amazon.com/dynamodb/), [AWS SNS](https://aws.amazon.com/sns/) and [AWS SQS](https://aws.amazon.com/sqs/).

It is valid, but not useful, to specify the same value for each. 

AWS ARNs for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) and [AWS SQS](https://aws.amazon.com/sqs/) are the preferred value since they

1. enable retry with backoff
2. other settings kick off retries, but with no backoff, since there is not way to delay a [AWS Kinesis](https://aws.amazon.com/kinesis/) or [AWS SNS](https://aws.amazon.com/sns/) message.

## Environment

* `settings.PRIMARY_ENVIRONMENT_SOURCE` controls the primary location for environment messages. Valid values are AWS ARNs for [AWS DynamoDB](https://aws.amazon.com/dynamodb/).
* `settings.SECONDARY_ENVIRONMENT_SOURCE` controls the secondary/failover location for environment messages. Valid values are AWS ARNs for [AWS DynamoDB](https://aws.amazon.com/dynamodb/).

It is valid, but not useful, to specify the same value for each. 

AWS ARN for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) is the preferred value since it 

1. persists even when the state machine dies

## Cache

* `settings.PRIMARY_CACHE_SOURCE` controls the primary location for cache messages. Valid values are AWS ARNs for [Elasticache](https://aws.amazon.com/elasticache/) and [AWS DynamoDB](https://aws.amazon.com/dynamodb/).
* `settings.SECONDARY_CACHE_SOURCE` controls the secondary/failover location for cache messages. Valid values are AWS ARNs for [Elasticache](https://aws.amazon.com/elasticache/) and [AWS DynamoDB](https://aws.amazon.com/dynamodb/).

It is valid, but not useful, to specify the same value for each. 

AWS ARN for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) is the preferred value since it 

1. persists even when the state machine dies

## Metrics

* `settings.PRIMARY_METRICS_SOURCE` controls the primary location for metrics messages. Valid values are AWS ARNs for [AWS CloudWatch](https://aws.amazon.com/cloudwatch/).
* `settings.SECONDARY_METRICS_SOURCE` metrics failover is currently **NOT SUPPORTED**

AWS ARN for [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) is the preferred value since it 

1. is a tightly integrated AWS custom metrics solution

[<< Justification](JUSTIFICATION.md) | [Chaos >>](CHAOS.md)
