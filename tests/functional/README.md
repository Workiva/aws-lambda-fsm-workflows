Running the Functional Tests
============================

Functional tests run against a set of Docker containers that mimic the AWS stack this service runs on
in production.

```bash
$ docker pull hello-world
$ docker build -t fsm_docker_runner -f tools/experimental/Dockerfile.fsm_docker_runner .
$ docker build -t dev_ecs -f tools/experimental/Dockerfile.dev_ecs .
$ docker build -t fsm -f tests/functional/Dockerfile .
```

# Python 2.X Tests

To run the functional tests with a Python 2.X image, do this:

```bash
# in one terminal
$ ENV_PYTHON=/usr/bin/python docker-compose -f tests/functional/docker-compose.yaml up fsm

# in another terminal
$ workon py2venv
$ make functional
```

# Python 3.X Tests

To run the functional tests with a Python 3.X image, do this:

```bash
# in one terminal
$ ENV_PYTHON=/usr/bin/python3 docker-compose -f tests/functional/docker-compose.yaml up fsm

# in another terminal
$ workon py3venv
$ make functional
```

PYTHONPATH=. python tools/start_state_machine.py --machine_name=ecs --initial_context='{"clone_aws_credentials":true,"task_details":{"run":{"container_image":"hello-world","cluster_arn":"arn:aws:ecs:testing:account:cluster/aws-lambda-fsm"},"run2":{"container_image":"hello-world","cluster_arn":"arn:aws:ecs:testing:account:cluster/aws-lambda-fsm"}}}'
