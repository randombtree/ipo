# Intelligent Container (ICON) python orchestrator (IPO)

IPO is a simple, proof-of-concept edge computing orchestrator for the ICON paradigm.
See [BLOMGREN, Roger Arne. Implementing and evaluating an ICON orchestrator. 2022] for details.

IPO is not suitable to be run in an open network as it lacks ALL and ANY security measures!

## Running IPO

See `testnet/README.md` for most details about running IPO in a test environment.

### Quick reference

- Start
    > `./bin/ipo daemon start`
- Run application (with internal and external port 8080).
    > `./bin/ipo container run iconsrv -p 8080:8080 -e PORT=8080`
- Bootstrap new node (i.e. make it visible to the other IPO nodes)
    > `./bin/ipo daemon bootstrap 10.1.0.253 1337`
- Help
    >  `./bin/ipo [command] -h`
