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

# AWS Lambda Finite State Machine Workflows

(This repo is maintained by an **SSAE 16 SOC 1 Type 2** compliant organization)

A Python 2.7 framework for running Finite State Machine (FSM) Workflows on 

1. [AWS Lambda](https://aws.amazon.com/lambda/) for code execution,
1. (Optionally/Experimental) [AWS ECS](https://aws.amazon.com/ecs/) for long-running code execution,
1. (Optionally) [AWS Kinesis](https://aws.amazon.com/kinesis/) for event dispatch
1. (Optionally) [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for event dispatch
1. (Optionally) [AWS SNS](https://aws.amazon.com/sns/) for event dispatch
1. (Optionally) [AWS SQS](https://aws.amazon.com/sqs/) for event dispatch
1. Support for primary and secondary event dispatch mechanisms
1. (Optionally) [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for checkpointing
1. (Optionally) [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for retries with backoff
1. (Optionally) [AWS SQS](https://aws.amazon.com/sqs/) for retries with backoff
1. (Optionally) [Memcache](https://aws.amazon.com/elasticache/) for idempotency
1. (Optionally) [Redis](https://aws.amazon.com/elasticache/) for idempotency
1. (Optionally) [AWS DynamoDB](https://aws.amazon.com/dynamodb/) for idempotency
1. (Optionally) [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) for error and failure monitoring

or 

1. [AWS Lambda](https://aws.amazon.com/lambda/) for code execution
1. (Experimental) [AWS Step Functions](https://aws.amazon.com/step-functions/) for orchestration and retries,

The FSM implementation is inspired by the paper:

[1] J. van Gurp, J. Bosch, "On the Implementation of Finite State Machines", in Proceedings of the 3rd Annual IASTED
    International Conference Software Engineering and Applications,IASTED/Acta Press, Anaheim, CA, pp. 172-178, 1999.
    (www.jillesvangurp.com/static/fsm-sea99.pdf)

1. [Architecture](docs/ARCHITECTURE.md)
1. [Overview](docs/OVERVIEW.md)
1. [Justification](docs/JUSTIFICATION.md)
1. [Settings](docs/SETTINGS.md)
1. [Chaos](docs/CHAOS.md)
1. [Idempotency](docs/IDEMPOTENCY.md)
1. [FSM YAML](docs/YAML.md)
1. [Running Locally](docs/LOCAL.md)
1. [Running on AWS](docs/AWS.md)
1. [Running on AWS Step Functions](docs/STEP.md)
1. [Setup AWS Services](docs/SETUP.md)
1. [TODO:](docs/TODO.md)


    
