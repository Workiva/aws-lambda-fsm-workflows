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

[<< Overview](OVERVIEW.md) | [Settings >>](SETTINGS.md)

# Justification

Finite State Machines are a model of mathematical computation that maps very well to a broad array of problems. 
The [Wikipedia page](https://en.wikipedia.org/wiki/Finite-state_machine) has a lot of very technical mathematical
details. From a practical/engineering standpoint, modeling a process as a state machine provides a consistent and well 
understood pattern for communicating the behaviour of a complex system. 

van Gurp's FSM object model has several key benefits. There are certainly other models, but I've found it very easy 
to implement and extend, across multiple different languages and technology stacks. The benefits outlined in the
paper, and those found out through practical implementation are outlined in the following sections.

## 1. Code is decoupled from FSM structure

Take for instance a process that needs to send an email and also send an sms message. You can define two
action classes that implement email and sms sending.

```python
class SendEmailAction(action):
  def execute(self, context, obj):
    send_email(context['to_email'])
    return "done"
```
        
```python
class SendSMSAction(action):
  def execute(self, context, obj):
    send_sms(context['to_sms'])
    return "done"
```

However, these actions can be hooked together in arbitrary order via configuration **WITHOUT ALTERING THE CODE**. 
This helps in code organisation and import efficiency.

## 2. Only action code needs to be implemented, not FSM driving code

Business logic never has to worry about sticking an event in Kinesis, handling failures etc. All of this framework
logic is handled by the library. **All the code needs to due is ensure it is idempotent in the case of retries**.

## 3. Code actions can be easily re-used

Certain kinds of actions lend themselves to re-use. Actions that copy data from a source location to a destination
can easily be made generic and re-used across many different machine definitions. Many such examples exists.

## 4. Common handling of FSM Context

The use of a dictionary to represent the context of the state machine is useful. As long as the context remains
JSON serializable, and a reasonable size, then pretty much anything can go into the context. 

## 5. Emphasizes small, testable code

FSM actions should be nothing more than thin shims on well-tested business logic. 

```python
class MyAction(action):

  def execute(self, context, obj):
  
    # step 1) pull needed data from the context
    arg1 = context['arg1']
    arg2 = context['arg2']
    
    # step 2) call your (well unit tested) business logic
    result = my_business_logic(arg1, arg2)
    
    # step 3) put the result back in the context for subsequent states
    context['arg3'] = result
    
    # step 4) return an event to drive the state machine forward
    return "done"
```

## 6. Single source of truth for "how it works"

Spaghetti is possible using any technology, but just being able to generate a picture of how the process works 
makes this pattern worthwhile. The configuration file (fsm.yaml) becomes the single source for execution of
the entire workflow.

# Examples

## Diagrams

The following diagrams are closely related to [UML State Diagram](https://en.wikipedia.org/wiki/State_diagram_(UML)) 
(within the limits of GraphViz). 

![UML](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22description%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22StateName1%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BStateName1%7Centry%2F+full.path.of.CodeClassToRunOnEntry%5Cldo%2F+full.path.of.CodeClassToRunInState%5Clexit%2F+full.path.of.CodeClassToRunOnExit%7D%22%5D%3B%0A%22__start__%22+-%3E+%22StateName1%22+%5Blabel%3D%22%22%5D%0A%22StateName1%22+-%3E+%22StateName2%22+%5Blabel%3D%22event%22%5D%3B%0A%22StateName2%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BStateName2%7Centry%2F+full.path.of.CodeClassToRunOnEntry%5Cldo%2F+full.path.of.CodeClassToRunInState%5Clexit%2F+full.path.of.CodeClassToRunOnExit%7D%22%5D%3B%0A%22StateName2%22+-%3E+%22__end__%22+%5Blabel%3D%22%22%5D%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%7D)

[<< Overview](OVERVIEW.md) | [Settings >>](SETTINGS.md)
