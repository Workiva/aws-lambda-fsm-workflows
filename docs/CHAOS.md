# Chaos

In the spirit of [Netflix](https://www.netflix.com/)'s [Chaos Monkey](https://github.com/Netflix/SimianArmy/wiki/Chaos-Monkey), 
this framework has the ability to inject failures into service calls to help simulate problems, and aid workflow authors
in writing robust, idempotent services.

In `settings(local).py` add a dictionary called `AWS_CHAOS`. This dictionary maps AWS services to a dictionary of
exceptions and approximate failure percentages (0.0-1.0). 0.5 means approximate 50% of calls to the given service
will fail.

For example

    from botocore.exceptions import ClientError
    
    AWS_CHAOS = {
        'kinesis': {
            ClientError({'Error': {'Code': 404, 'Message': 'AWS Chaos'}}, 'kinesis'): 0.1,
        },
        'dynamodb': {
            ClientError({'Error': {'Code': 404, 'Message': 'AWS Chaos'}}, 'dynamodb'): 0.1,
        },
        'memcache': {
            False: 0.1
        },
        'cloudwatch': {
            ClientError({'Error': {'Code': 404, 'Message': 'AWS Chaos'}}, 'cloudwatch'): 0.0,
        },
    }
    
This works locally and when deployed to AWS.