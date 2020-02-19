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

# Summary

A Python 2.7 and 3.6 compatible framework for running Finite State Machine (FSM) Workflows on 

1. [AWS Lambda](https://aws.amazon.com/lambda/) for code execution,
1. (Optionally/Experimental) [AWS ECS](https://aws.amazon.com/ecs/) for long-running code execution,
1. (Optionally) [AWS SQS](https://aws.amazon.com/sqs/), [AWS Kinesis](https://aws.amazon.com/kinesis/), [AWS SNS](https://aws.amazon.com/sns/), or [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for event dispatch
1. Support for primary and secondary event dispatch mechanisms
1. (Optionally) [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for checkpointing
1. (Optionally) [AWS SQS](https://aws.amazon.com/sqs/), or [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for retries with backoff
1. (Optionally) [Redis](https://aws.amazon.com/elasticache/), [Memcache](https://aws.amazon.com/elasticache/), or [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for idempotency
1. (Optionally) [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) for error and failure monitoring
1. (Experimental) [AWS Step Functions](https://aws.amazon.com/step-functions/) for orchestration and retries

# Links

1. [Documentation](docs/OVERVIEW.md)
1. [PyPI Page](https://pypi.org/project/aws-lambda-fsm/)


This repository is managed to Workiva’s SSAE 16 SOC 1 Type 2/SOC 2 Type 2 standards and is deployed within a platform that is authorized to operate at FedRAMP Moderate.
