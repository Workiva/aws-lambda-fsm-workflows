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

![saga](https://chart.googleapis.com/chart?cht=gv&chl=digraph+G+%7B%0Alabel%3D%22saga%22%0Alabelloc%3D%22t%22%0A%22__start__%22+%5Blabel%3D%22start%22%2Cshape%3Dcircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%22StartSaga%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BStartSaga%7Cdo%2F+examples.saga.actions.StartSagaAction%7D%22%5D%3B%0A%22__start__%22+-%3E+%22StartSaga%22+%5Blabel%3D%22%22%5D%0A%22StartSaga%22+-%3E+%22Car%22+%5Blabel%3D%22done%22%5D%3B%0A%22StartSaga%22+-%3E+%22Fail%22+%5Blabel%3D%22fail%22%5D%3B%0A%22Car%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BCar%7Cdo%2F+examples.saga.actions.CarAction%7D%22%5D%3B%0A%22Car%22+-%3E+%22Hotel%22+%5Blabel%3D%22done%22%5D%3B%0A%22Car%22+-%3E+%22Fail%22+%5Blabel%3D%22fail%22%5D%3B%0A%22Hotel%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BHotel%7Cdo%2F+examples.saga.actions.HotelAction%7D%22%5D%3B%0A%22Hotel%22+-%3E+%22Plane%22+%5Blabel%3D%22done%22%5D%3B%0A%22Hotel%22+-%3E+%22Fail%22+%5Blabel%3D%22fail%22%5D%3B%0A%22Plane%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BPlane%7Cdo%2F+examples.saga.actions.PlaneAction%7D%22%5D%3B%0A%22Plane%22+-%3E+%22EndSaga%22+%5Blabel%3D%22done%22%5D%3B%0A%22Plane%22+-%3E+%22Fail%22+%5Blabel%3D%22fail%22%5D%3B%0A%22EndSaga%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BEndSaga%7Cdo%2F+examples.saga.actions.EndSagaAction%7D%22%5D%3B%0A%22EndSaga%22+-%3E+%22__end__%22+%5Blabel%3D%22%22%5D%0A%22Fail%22+%5Bshape%3DMrecord%2Clabel%3D%22%7BFail%7Cdo%2F+examples.saga.actions.FailAction%7D%22%5D%3B%0A%22Fail%22+-%3E+%22__end__%22+%5Blabel%3D%22%22%5D%0A%22__end__%22+%5Blabel%3D%22end%22%2Cshape%3Ddoublecircle%2Cstyle%3Dfilled%2Cfillcolor%3Dblack%2Cfontcolor%3Dwhite%2Cfontsize%3D9%5D%3B%0A%7D)
