Running the Functional Tests
==

```bash
> docker build -t fsm -f tests/functional/Dockerfile .
> docker build -t fsm_docker_runner -f tools/experimental/Dockerfile.fsm_docker_runner .
> docker build -t dev_ecs -f tools/experimental/Dockerfile.dev_ecs .
> docker run \
  -p 11211:11211 -p 6379:6379 -p 4568:4568 \
  -v /var/run/docker.sock:/var/run/docker.sock fsm \
  -e VOLUME=${PWD}/tests/functional/settings.py.functional:/usr/local/bin/settings.py \
  -e LINK=fsm \
  --hostname fsm \
  --name fsm \
  fsm
> make functional  # in another terminal
```

