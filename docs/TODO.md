# TODO:

    1. fsm.yaml validation tools
    1. handling FSM schema changes for in-flight machines
    1. aborting an FSM via ui
    1. monitoring an FSM vi ui
    1. DRY out the tools
    1. handle partial failures of batch puts on kinesis and dynamodb and sqs
    1. process to cleanup dynamodb after the kinesis stream has been aged out
    1. how to handle a task bomb filling the shards
