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
