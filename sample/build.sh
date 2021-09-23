#!/bin/sh
# Build docker image of 

test -d ./dist && rm -rf ./dist
./setup.py sdist
PACKET=$(basename ./dist/iconsrv-*.gz)
docker build --build-arg PACKET=$PACKET --tag iconsrv .
