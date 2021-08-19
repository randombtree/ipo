"""
ICOND server; starting and stopping.
"""

import sys
import asyncio
import os
import json
import uuid
import argparse
from typing import Union

import docker  # type: ignore

from . message import (IconMessage, InvalidMessage, JSONReader, JSONWriter)


ICOND_REPO = "icond_repository"   # ICON local repository
ICOND_CTL_SOCK = "/var/run/icond/icond.sock"   # ICON control socket


class Icond:
    """ Icond global state """
    def __init__(self):
        self.docker = docker.DockerClient(base_url='unix://var/run/docker.sock')


def iconctl_connection_factory(icond: Icond):
    """ Create a connection handler with icond included in the closure """
    async def iconctl_connection_handler(reader, writer):
        reader = JSONReader(reader)
        writer = JSONWriter(writer)
        while True:
            try:
                msg = await reader.read()
                print(f"Got line {msg}")
                msg = IconMessage(msg)
            except (json.JSONDecodeError, InvalidMessage) as e:
                reply = IconMessage(IconMessage.TYPE_ERROR, "connection", msg = e.msg)
                await writer.write(reply)
    return iconctl_connection_handler


class RPCHandler:
    """ RPC command handler base class """
    def __init__(self, stream_id: uuid.UUID):
        self.stream_id = stream_id

    def generate_message(self, msg_type: str, **contents):
        """ Generate a message with current ID attached """
        return IconMessage(msg_type, self.stream_id, **contents)


class OneShotRPCHandler(RPCHandler):
    """ Simple RPC handler that only returns a message """
    def handle(self, msg: IconMessage) -> IconMessage:
        pass


class StreamRPCHandler(RPCHandler):
    """ Persistent RPC handler that will exchange several messages with the same ID """
    RPC_NAME = "undefined"
    task: Union[None, asyncio.tasks.Task]
    in_queue: asyncio.Queue
    out_queue: asyncio.Queue

    def __init__(self, stream_id: uuid.UUID, out_queue: asyncio.Queue):
        super().__init__(stream_id)
        self.out_queue = out_queue
        self.in_queue = asyncio.Queue()
        self.task = None

    def start(self) -> asyncio.tasks.Task:
        """
        Start the RPC handler; this involves running the run method as a task

        Returns the task for the caller to wait on.
        """
        self.task =  asyncio.create_task(self.run(), name = f"rpchandler-{self.RPC_NAME}")
        return self.task

    def put(self, item):
        """
        Put an item to the tasks processing queue - this most probably is a message.
        """
        try:
            self.in_queue.put_nowait(item)
        except asyncio.QueueFull:
            # Task has failed somehow, kill it
            self.task.cancel("Task couldn't keep up with the messages")

    async def run(self):
        """ Async method to run as part of the handler """


async def iconctl_server(icond: Icond):
    """ ICON control channel server """
    os.makedirs(os.path.dirname(ICOND_CTL_SOCK), exist_ok = True)
    try:
        os.unlink(ICOND_CTL_SOCK)
    except OSError:
        if os.path.exists(ICOND_CTL_SOCK):
            raise

    server = await asyncio.start_unix_server(
        iconctl_connection_factory(icond),
        path = ICOND_CTL_SOCK
    )
    async with server:
        await server.serve_forever()


class InitializationException(Exception):
    ...


def init_repository(icond: Icond):
    print("Initializing repository..")
    # Repository
    try:
        repo = icond.docker.containers.get(ICOND_REPO)
        if repo.status != "running":
            print(f"ICON repository was not running ({repo.status}), starting it..")
            repo.start()
        else:
            print("ICON repository ok")
    except docker.errors.NotFound:
        # First run; create repository
        # TODO: separate init from normal daemon run
        print("Creating ICON local repository..")
        icond.docker.containers.run(
            "registry:2",
            name=ICOND_REPO,
            detach=True,
            restart_policy={"name": "always"},
            ports={"5000/tcp": 5000}       # TODO: Config
        )
        print("Done..")
    except docker.errors.APIError as e:
        print("Failed to communicate with Docker")
        print(e)
        raise InitializationException('Failed to communicate with Docker') from e


async def main():
    """ Icond daemon """
    icond = Icond()

    init_repository(icond)
    print("Starting server")
    # Start the control channel server
    ctl_server = asyncio.create_task(iconctl_server(icond),
                                     name = "ctl_server")
    # Exit on first completetion
    done, pending = await asyncio.wait(
        (ctl_server,),
        return_when = asyncio.FIRST_COMPLETED)

def start(params : argparse.Namespace):
    if not (params.force or os.geteuid() == 0):
        print("Starting of the ICON daemon might fail when not run as root...")
        print("Try --force if you are confident it will work")
        sys.exit(-1)

    asyncio.run(main())
