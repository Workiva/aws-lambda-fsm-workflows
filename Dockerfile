# Copyright 2016 Workiva Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu

# install linux stuff
RUN apt-get update
RUN apt-get install -y python-pip

# install python stuff
ADD requirements.txt requirements.txt
ADD requirements_shipyard.txt requirements_shipyard.txt
RUN pip install -r requirements.txt
RUN pip install -r requirements_shipyard.txt

# add source code and scripts
ADD aws_lambda_fsm aws_lambda_fsm
ADD tools/fsm_docker_runner.py fsm_docker_runner.py
ADD tools/fsm_sqs_to_arn.py fsm_sqs_to_arn.py

# set the cmd
CMD ["/bin/bash", "-c", "touch settings.py; if [ ${SQS2ARN} ]; then python fsm_sqs_to_arn.py; else python fsm_docker_runner.py; fi"]
