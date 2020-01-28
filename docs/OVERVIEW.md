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

[<< Architecture](ARCHITECTURE.md) | [Justification >>](JUSTIFICATION.md)

# Overview

A framework for running Finite State Machines (FSMs) on 
[AWS Lambda](https://aws.amazon.com/lambda/),
[AWS Kinesis](https://aws.amazon.com/kinesis/), 
[AWS DynamoDB](https://aws.amazon.com/dynamodb/),
[AWS SNS](https://aws.amazon.com/sns/),
[AWS SQS](https://aws.amazon.com/sqs/),
[AWS CloudWatch](https://aws.amazon.com/cloudwatch/), 
and [AWS Elasticache](https://aws.amazon.com/elasticache/). 

The FSM implementation is inspired by the paper:

[1] J. van Gurp, J. Bosch, "On the Implementation of Finite State Machines", in Proceedings of the 3rd Annual IASTED
    International Conference Software Engineering and Applications,IASTED/Acta Press, Anaheim, CA, pp. 172-178, 1999.
    (www.jillesvangurp.com/static/fsm-sea99.pdf)

## Define Your Process

You probably already know what your process looks like.

![FSM](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22overview%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22FirstState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BFirstState%7Centry%2F+Action1%5Cldo%2F+Action2%5Clexit%2F+Action3%7D%22%5D%3B%0A%22__start__%22+-%3E+%22FirstState%22%0A%22FirstState%22+-%3E+%22SecondState%22+%5Blabel%3D%22done%22%5D%3B%0A%22FirstState%22+-%3E+%22ThirdState%22+%5Blabel%3D%22skip%22%5D%3B%0A%22SecondState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BSecondState%7Centry%2F+Action4%5Cldo%2F+Action5%5Clexit%2F+Action6%7D%22%5D%3B%0A%22SecondState%22+-%3E+%22ThirdState%22+%5Blabel%3D%22done%22%5D%3B%0A%22ThirdState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BThirdState%7Centry%2F+Action7%5Cldo%2F+Action8%5Clexit%2F+Action9%7D%22%5D%3B%0A%22ThirdState%22+-%3E+%22__end__%22%0A%7D)

## Write Your Code

And probably already have some code that you can import and wrap in a thin Action shim.

```python
from aws_lambda_fsm.action import Action
from my.product import my_fancy_function
from my.product import my_other_fancy_function

class Action2(Action):
  def execute(self, context, obj):
    arg1 = context['arg1']
    mff = my_fancy_function(arg1)
    context['mff'] = mff # store the result in the context for subsequent states
    return "done"
    
class Action5(Action):
  def execute(self, context, obj):
    mff = context['mff'] # grab result of last state from the context
    my_other_fancy_function(mff)
    return "done"
```

## Hook Up Your FSM

All you need to do is configure your state machine, and aws-lambda-fsm will take care of executing it.

```yaml
machines:
- name: Example

  states:

  - name: FirstState
    initial: true
    entry_action: path.to.Action1
    do_action: path.to.Action2
    exit_action: path.to.Action3
    transitions:
    - target: SecondState
      event: done
    - target: ThirdState
      event: skip

  - name: SecondState
    entry_action: path.to.Action4
    do_action: path.to.Action5
    exit_action: path.to.Action6
    transitions:
    - target: ThirdState
      event: done

  - name: ThirdState
    final: true
    entry_action: path.to.Action7
    do_action: path.to.Action8
    exit_action: path.to.Action9
```
        
## Lambda Calls

An [AWS Lambda](https://aws.amazon.com/lambda/) function executes for each transition. A diagram and listing of each 
entry/exit/do action ran in each [AWS Lambda](https://aws.amazon.com/lambda/) function is shown below.

![FSM->Lambda](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22overview%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22FirstState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BFirstState%7Centry%2F+Action1%5Cldo%2F+Action2%5Clexit%2F+Action3%7D%22%5D%3B%0A%22__start__%22+-%3E+%22FirstState%22+%5Blabel%3D%22%28%CE%BB1%29+%22%5D%0A%22FirstState%22+-%3E+%22SecondState%22+%5Blabel%3D%22%28%CE%BB2%29+done%22%5D%3B%0A%22FirstState%22+-%3E+%22ThirdState%22+%5Blabel%3D%22%28%CE%BB3%29+skip%22%5D%3B%0A%22SecondState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BSecondState%7Centry%2F+Action4%5Cldo%2F+Action5%5Clexit%2F+Action6%7D%22%5D%3B%0A%22SecondState%22+-%3E+%22ThirdState%22+%5Blabel%3D%22%28%CE%BB4%29+done%22%5D%3B%0A%22ThirdState%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BThirdState%7Centry%2F+Action7%5Cldo%2F+Action8%5Clexit%2F+Action9%7D%22%5D%3B%0A%22ThirdState%22+-%3E+%22__end__%22+%5Blabel%3D%22%28%CE%BB5%29+%22%5D%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%7D)

λ1

```python
FirstState.entry_action
FirstState.do_action
```
 
λ2

```python
FirstState.exit_action
SecondState.entry_action 
SecondState.do_action
```
    
λ3

```python
FirstState.exit_action
ThirdState.entry_action 
ThirdState.do_action
```
  
λ4

```python
SecondState.exit_action
ThirdState.entry_action 
ThirdState.do_action
```
 
λ5

```python
ThirdState.exit_action
```
 
[<< Architecture](ARCHITECTURE.md) | [Justification >>](JUSTIFICATION.md)
