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
from . import message
from . eventqueue import GlobalEventQueue, Subscription

ICOND_REPO = "icond_repository"   # ICON local repository
ICOND_CTL_SOCK = "/var/run/icond/icond.sock"   # ICON control socket

class ShutdownEvent:
    ...

class Icond:
    """ Icond global state """
    shutdown: bool
    eventqueue: GlobalEventQueue

    def __init__(self):
        self.docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = GlobalEventQueue()

    def do_shutdown(self):
        """ Shutdown daemon commanded """
        self.shutdown = True
        self.eventqueue.publish(ShutdownEvent())

    def subscribe_event(self, event: type) -> Subscription:
        """ Subscribe to icond events """
        return self.eventqueue.subscribe(event)


async def waitany(tset: set[asyncio.Task]) -> tuple[asyncio.Task, asyncio.Task]:
    """ Convinience function to wait for any async task completion """
    return await asyncio.wait(tset, return_when = asyncio.FIRST_COMPLETED)


async def iconctl_connection_handler(reader, writer, icond: Icond):
    """ Icon control channel handler """
    reader = JSONReader(reader)
    writer = JSONWriter(writer)
    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        # The shutdown message helps us to tear down the connection
        # while waiting for peer actions (i.e. reading, writing)
        shutdown_task = asyncio.create_task(shutdown_event.get())
        while not icond.shutdown:
            reply_msg = None
            try:
                print("Reading...")
                read_task = asyncio.create_task(reader.read())
                (done, _pending) = await waitany({shutdown_task, read_task})
                if shutdown_task in done:
                    assert isinstance(shutdown_task.result(), ShutdownEvent)
                    break
                assert read_task in done
                e = read_task.exception()
                if e is not None:
                    # Task threw exception, connection probably died
                    print(f'Connection error {e}')
                    break
                msg = read_task.result()
                print(f'Got line {msg}')
                msg = IconMessage.from_dict(msg)
                print(f'Received message: {msg}')
                if isinstance(msg, message.Shutdown):
                    print('Shutting down')
                    reply_msg = msg.create_reply(msg = 'Shutting down')
                    icond.do_shutdown()
                elif isinstance(msg, message.ContainerRun):
                    print(f'Run container {msg.image}')
                    # TODO!:
                    # Docker commands are synchronous, so some
                    # threading will be needed here; doing some bad blocking
                    reply_msg = msg.create_reply(msg = 'Working..')
                else:
                    print(f'Not handling message {msg}')
            except (json.JSONDecodeError, InvalidMessage) as e:
                print(f'Invalid message: {e.msg}')
                reply_msg = IconMessage(IconMessage.TYPE_ERROR, "connection", msg = e.msg)

            if reply_msg is None:
                continue
            print(f'Sending reply {reply_msg}')
            reply_task = asyncio.create_task(writer.write(reply_msg))
            (done, _pending) = await waitany({shutdown_task, reply_task})
            if shutdown_task in done:
                break
            assert reply_task in done
            e = read_task.exception()
            if e is not None:
                print(f'Exception writing reply {e}')
                break


def iconctl_connection_factory(icond: Icond):
    """ Create a connection handler with icond included in the closure """

    async def connection_handler(reader, writer):
        return await iconctl_connection_handler(reader, writer, icond)
    return connection_handler


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
    """ Exception inicating something went wrong during initialization """
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
    ctl_server_task = asyncio.create_task(iconctl_server(icond),
                                          name = "ctl_server")

    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        shutdown_task = asyncio.create_task(shutdown_event.get())
        (done, _pending) = await waitany({shutdown_task, ctl_server_task})
        # Any task finishing indicates that we want to exit, either due to some internal
        # error or a shutdown event
        if shutdown_task in done:
            print("Shutdown signaled")


def start(params : argparse.Namespace):
    """ Entry point for module run """
    if not (params.force or os.geteuid() == 0):
        print("Starting of the ICON daemon might fail when not run as root...")
        print("Try --force if you are confident it will work")
        sys.exit(-1)

    asyncio.run(main())
