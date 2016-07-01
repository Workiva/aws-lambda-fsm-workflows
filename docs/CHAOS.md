# Chaos

In the spirit of [Netflix](https://www.netflix.com/)'s [Chaos Monkey](https://github.com/Netflix/SimianArmy/wiki/Chaos-Monkey), 
this framework has the ability to inject failures into service calls to help simulate problems, and aid workflow authors
in writing robust, idempotent services.

In `settings(local).py` add a dictionary called `AWS_CHAOS`. This dictionary maps AWS services to a dictionary of
exceptions and approximate failure percentages (0.0-1.0). 0.5 means approximate 50% of calls to the given service
will fail.

For example

    PRIMARY_STREAM_SOURCE = 'arn:aws:kinesis:eu-west-1:663511558366:stream/aws-lambda-fsm'
    PRIMARY_CACHE_SOURCE = 'arn:aws:dynamodb:eu-west-1:663511558366:table/aws-lambda-fsm.cache'

    from botocore.exceptions import ClientError
    
    AWS_CHAOS = {
        PRIMARY_STREAM_SOURCE': {
            ClientError({'Error': {'Code': 404, 'Message': 'AWS Chaos'}}, 'service'): 0.1,
        },
        PRIMARY_CACHE_SOURCE: {
            ClientError({'Error': {'Code': 404, 'Message': 'AWS Chaos'}}, 'service'): 0.1,
        }
    }
    
This works locally and when deployed to AWS.