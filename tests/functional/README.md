Running the Functional Tests
==

```bash
> docker build -t fsm -f tests/functional/Dockerfile .
> docker run -p 11211:11211 -p 6379:6379 -p 4568:4568 fsm
> make functional  # in another terminal
```

