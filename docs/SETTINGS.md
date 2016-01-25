# Settings

The behaviour of the framework is controlled by a few important settings

## Event Streaming

* `settings.PRIMARY_STREAM_SOURCE` controls the primary location for event messages. Valid values are AWS ARNs for `kinesis`, `dynamodb`, `sns` and `sqs`.
* `settings.SECONDARY_STREAM_SOURCE` controls the secondary/failover location for event messages. Valid values are AWS ARNs for`kinesis`, `dynamodb`, `sns` and `sqs`.

It is valid, but not useful, to specify the same value for each. 

AWS ARN for `kinesis` is the preferred value since it 

1. keeps a record of the entire history of the state machine
2. allows a stalled state machine to be restored from a checkpoint and replayed
3. append-only logs and state machines are very, very related [logs](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) & [fsm](https://www.cs.cornell.edu/fbs/publications/smsurvey.pdf)

## Checkpointing

* `settings.PRIMARY_CHECKPOINT_SOURCE` controls the primary location for checkpoint messages. Valid values are AWS ARNs for `dynamodb`.
* `settings.SECONDARY_CHECKPOINT_SOURCE` controls the secondary/failover location for checkpoint messages. Valid values are AWS ARNs for `dynamodb`.

It is valid, but not useful, to specify the same value for each. 

AWS ARN for `dynamodb` is the preferred value since it 

1. persists even when the state machine dies

## Retries

* `settings.PRIMARY_RETRY_SOURCE` controls the primary location for retry messages. Valid values are AWS ARNs for `kinesis`, `dynamodb`, `sns` and `sqs`.
* `settings.SECONDARY_RETRY_SOURCE` controls the secondary/failover location for retry messages. Valid values are AWS ARNs for `kinesis`, `dynamodb`, `sns` and `sqs`.

It is valid, but not useful, to specify the same value for each. 

AWS ARNs for `dynamodb` and `sqs` are the preferred value since they

1. enable retry with backoff
2. other settings kick off retries, but with no backoff, since there is not way to delay a `kinesis` or `sns` message.

## Environment

* `settings.PRIMARY_ENVIRONMENT_SOURCE` controls the primary location for environment messages. Valid values are AWS ARNs for `dynamodb`.
* `settings.SECONDARY_ENVIRONMENT_SOURCE` controls the secondary/failover location for environment messages. Valid values are AWS ARNs for `dynamodb`.

It is valid, but not useful, to specify the same value for each. 

AWS ARN for `dynamodb` is the preferred value since it 

1. persists even when the state machine dies

## Cache

* `settings.PRIMARY_CACHE_SOURCE` controls the primary location for cache messages. Valid values are AWS ARNs for `elasticache` and `dynamodb`.
* `settings.SECONDARY_CACHE_SOURCE` cache failover is currently **NOT SUPPORTED**

It is valid, but not useful, to specify the same value for each. 

AWS ARN for `dynamodb` is the preferred value since it 

1. persists even when the state machine dies

## Metrics

* `settings.PRIMARY_METRICS_SOURCE` controls the primary location for metrics messages. Valid values are AWS ARNs for `cloudwatch`.
* `settings.SECONDARY_METRICS_SOURCE` metrics failover is currently **NOT SUPPORTED**

AWS ARN for `cloudwatch` is the preferred value since it 

1. is a tightly integrated AWS custom metrics solution