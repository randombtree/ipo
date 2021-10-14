#!/bin/sh
# Temporary initialization helper
# (to be replaced later with proper initialization of the daemon)
# - Builds an installable python package of ICON for the icon client
#   (currently using an unified icon package)
# - Builds the base ICON docker image that has the necessary ICON bits installed
# - Publishes the image in the local registry.

# If something fails midway just stop..
set -e

PROJDIR=$(dirname $0|xargs dirname)
cd $PROJDIR


# Build installable packet
./setup.py sdist
# Build image with the icon package installed
docker build --tag icon/bullseye .

# Tag it for local repo
docker image tag icon/bullseye:latest localhost:5000/ipo/icon:latest

SUDO=""
euid=$(test x$EUID != x && echo $EUID || id -u)
if [ $euid -ne 0 ];then
    read -p "Docker probably needs root; use it [y/N]?" answer
    if [ x$answer = xy ] || [ x$answer = xY ];then
	SUDO="sudo"
    fi
fi
# Use docker directly (ipo cmd also does it, but uses internal api) to init and push
$SUDO sh -c 'bin/ipo misc init_registry && docker image push localhost:5000/ipo/icon:latest'

# Quck check if it seems to work
repos=$(curl http://localhost:5000/v2/_catalog 2>/dev/null|grep -c 'icon')
if [ $repos -eq 0 ];then
    echo "Somehow failed to insert ICON image into repo?"
fi
