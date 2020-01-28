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

[<< Idempotency](IDEMPOTENCY.md) | [Running Locally >>](LOCAL.md)

# YAML Format

```yaml
machines:                                              # heading for multiple machines

- name: machine_name                                   # identifiable name for the machine

  states:                                              # heading for multiple states

  - name: state1                                       # name of the state
    entry_action: module.EntryActionClass              # class name of the action code to execute on entry
    do_action: module.DoActionClass                    # class name of the action code to execute on arrival
    exit_action: module.ExitActionClass                # class name of the action code to execute on exit
    initial: true                                      # true for the initial state of the machine
    
    transitions:                                       # heading for multiple transitions
    
    - target: state2                                   # destination/target state for the transition
      action: module.TransitionActionClass             # class name of the action code to execute on transition
      event: event1                                    # event that trigger the transition
    - target: final
      event: done

  - name: state2
    entry_action: module.AnotherEntryActionClass
    do_action: module.AnotherDoActionClass
    exit_action: module.AnotherExitActionClass
    
    transitions:
    
    - target: state1
      action: module.AnotherTransitionActionClass
      event: event1
    - target: final
      event: done

  - name: final
    do_action: module.YetAnotherDoActionClass
    final: true                                        # true for the final/terminal state(s) of the machine      
```                  

![image](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22machine_name%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22state1%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bstate1%7Centry%2F+module.EntryActionClass%5Cldo%2F+module.DoActionClass%5Clexit%2F+module.ExitActionClass%7D%22%5D%3B%0A%22__start__%22+-%3E+%22state1%22+%5Blabel%3D%22%22%5D%0A%22state1%22+-%3E+%22state2%22+%5Blabel%3D%22event1%2F+module.TransitionActionClass%22%5D%3B%0A%22state1%22+-%3E+%22final%22+%5Blabel%3D%22done%22%5D%3B%0A%22state2%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bstate2%7Centry%2F+module.AnotherEntryActionClass%5Cldo%2F+module.AnotherDoActionClass%5Clexit%2F+module.AnotherExitActionClass%7D%22%5D%3B%0A%22state2%22+-%3E+%22state1%22+%5Blabel%3D%22event1%2F+module.AnotherTransitionActionClass%22%5D%3B%0A%22state2%22+-%3E+%22final%22+%5Blabel%3D%22done%22%5D%3B%0A%22final%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bfinal%7Cdo%2F+module.YetAnotherDoActionClass%7D%22%5D%3B%0A%22final%22+-%3E+%22__end__%22+%5Blabel%3D%22%22%5D%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%7D)

[<< Idempotency](IDEMPOTENCY.md) | [Running Locally >>](LOCAL.md)
