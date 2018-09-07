Running the Functional Tests
============================

Functional tests run against a set of Docker containers that mimic the AWS stack this service runs on
in production.

# Python 2.7

To run the functional tests with a Python 2.7 image, do this:

```bash
$ docker build -t fsm_docker_runner -f tools/experimental/Dockerfile.fsm_docker_runner .
$ docker build -t dev_ecs -f tools/experimental/Dockerfile.dev_ecs .
$ docker build -t fsm -f tests/functional/Dockerfile .
$ docker container rm fsm
$ docker run \
  -p 11211:11211 -p 6379:6379 -p 4568:4568 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e VOLUME=${PWD}/tests/functional/settings.py.functional:/usr/local/bin/settings.py \
  -e LINK=fsm \
  -e RUNNER=fsm_docker_runner:latest \
  --hostname fsm \
  --name fsm \
  fsm

# in another terminal
$ make functional
```

The `docker container rm fsm` above is necessary on a second run when including the '--name fsm' option
below to avoid conflicts. Functional tests will run fine without naming the container, though there are some 
(as yet untested) failures in the container without a name

# Python 3.6

There is a variant of the main image that is built with Python 3. 
To run the functional tests with a Python 3.6 image, do this:

```bash
$ docker build -t fsm_docker_runner_py3 -f tools/experimental/Dockerfile.fsm_docker_runner_py3 .
$ docker build -t dev_ecs_py3 -f tools/experimental/Dockerfile.dev_ecs_py3 .
$ docker build -t fsm_py3 -f tests/functional/Dockerfile_py3 .
$ docker container rm fsm_py3
$ docker run \
  -p 11211:11211 -p 6379:6379 -p 4568:4568 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e VOLUME=${PWD}/tests/functional/settings.py.functional:/usr/local/bin/settings.py \
  -e LINK=fsm_py3 \
  -e RUNNER=fsm_docker_runner_py3:latest \
  --hostname fsmpy3 \
  --name fsm_py3 \
  fsm_py3
```
