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

![tracer](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22tracer%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22state1%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bstatevent1%7Centry%2F+aws_lambda_fsm.action.Action%5Cldo%2F+examples.tracer.actions.IncrementAction%5Clexit%2F+aws_lambda_fsm.action.Action%7D%22%5D%3B%0A%22__start__%22+-%3E+%22statevent1%22+%5Blabel%3D%22%22%5D%0A%22statevent1%22+-%3E+%22state2%22+%5Blabel%3D%22event1%2F+aws_lambda_fsm.action.Action%22%5D%3B%0A%22statevent1%22+-%3E+%22final%22+%5Blabel%3D%22done%22%5D%3B%0A%22state2%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bstate2%7Centry%2F+aws_lambda_fsm.action.Action%5Cldo%2F+examples.tracer.actions.IncrementAction%5Clexit%2F+aws_lambda_fsm.action.Action%7D%22%5D%3B%0A%22state2%22+-%3E+%22statevent1%22+%5Blabel%3D%22event1%2F+aws_lambda_fsm.action.Action%22%5D%3B%0A%22state2%22+-%3E+%22final%22+%5Blabel%3D%22done%22%5D%3B%0A%22final%22+%5Bshape%3DMrecord%2Clabel%3D%22%7Bfinal%7C%7D%22%5D%3B%0A%22final%22+-%3E+%22__end__%22+%5Blabel%3D%22%22%5D%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%7D)
