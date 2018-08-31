Running the Functional Tests
============================

Functional tests run against a set of Docker containers that mimic the AWS stack this service runs on
in production.

To run the functional tests with a Python 2.7 image, do this:

```bash
$ docker build -t fsm_docker_runner -f tools/experimental/Dockerfile.fsm_docker_runner .
$ docker build -t dev_ecs -f tools/experimental/Dockerfile.dev_ecs .
$ docker build -t fsm -f tests/functional/Dockerfile .
$ docker run \
  -p 11211:11211 -p 6379:6379 -p 4568:4568 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e VOLUME=${PWD}/tests/functional/settings.py.functional:/usr/local/bin/settings.py \
  -e LINK=fsm \
  --hostname fsm \
  fsm

# in another terminal
$ make functional
```

There is a variant of the main image that is built with Python 3.
To run it, swap the last two `docker` commands above with these before
running `make functional`:

```bash
$ docker build -t fsm_py3 -f tests/functional/Dockerfile_py3 .
$ docker run \
  -p 11211:11211 -p 6379:6379 -p 4568:4568 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e VOLUME=${PWD}/tests/functional/settings.py.functional:/usr/local/bin/settings.py \
  -e LINK=fsm_py3 \
  --hostname fsm_py3 \
  fsm_py3
```
