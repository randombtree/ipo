"""
Container controller

Containers are run in docker, but the ICON orchestrator provides more services
to them.
"""
import asyncio
from typing import Union, cast
from enum import Enum
import os.path
import json
import logging

import docker  # type: ignore

from .. state import Icond
from ... util.asynctask import AsyncTask, AsyncTaskRunner
from .. events import (
    ShutdownEvent,
    ContainerRunningEvent,
)
from ...api import message
from ...api.message import (MessageReader, JSONWriter)


log = logging.getLogger(__name__)


# Container states
ContainerState = Enum('ContainerState', 'STOPPED STARTING CONWAITING RUNNING FAILED')


class Container:
    """ A running container we are providing ICON services to """
    name: str
    image: str
    icond: Icond
    task: Union[None, asyncio.Task]  # The running task
    state: ContainerState
    inqueue: asyncio.Queue
    task_runner: AsyncTaskRunner
    container_name: str
    control_path: str         # Path to mount into container
    control_socket_path: str  # Container control socket name
    socket_server: Union[asyncio.AbstractServer, None]
    clients: int              # Connected clients count

    def __init__(self, name: str, image: str, icond: Icond):
        """
        name: Container name
        icond: Daemon global state
        """
        self.name = name
        self.image = image
        self.icond = icond
        self.task = None
        self.state = ContainerState.STOPPED
        self.inqueue = asyncio.Queue()
        self.task_runner = AsyncTaskRunner()
        self.container_name = f'ICON_{self.name}'
        self.icond.eventqueue.listen(ShutdownEvent, self.inqueue)
        # Create container control path that is mounted inside the container NS
        control_path = f'{self.icond.config.run_directory}/{self.container_name}'
        if not os.path.exists(control_path):
            os.mkdir(control_path, mode = 0o700)
        # FIXME?: /else clean up all files there perhaps?
        self.control_path = control_path
        # Clean up old socket if there
        control_socket_path = f'{control_path}/icon.sock'
        if os.path.exists(control_socket_path):
            os.unlink(control_socket_path)  # If this throws we are screwed anyway
        self.control_socket_path = control_socket_path
        # Socket is initialized in async context when task is running
        self.socket_server = None
        self.clients = 0

    def start(self) -> asyncio.Task:
        """
        Start this container from an image
        image: Docker image (atm)
        """
        # TODO: It should be perfectly OK to concurrently try to start an ICON
        if not self.is_running():
            self.state = ContainerState.STARTING
            self.task = asyncio.create_task(self._run())
        else:
            # FIXME? Quirk to allow waiters on existing containers continue
            self.emit_state()
        assert self.task is not None
        return cast(asyncio.Task, self.task)

    def is_running(self):
        """ Is the container in some kind of running state/starting up """
        return not (self.state is ContainerState.STOPPED or self.state is ContainerState.FAILED)

    async def stop(self):
        # FIXME: We don't distinguish between them here, perhaps we should?
        if self.is_running():
            await self.inqueue.put(ShutdownEvent)
        self.task = None

    def emit_state(self, new_state = None):
        """ Send an appropriate event based on the current state """
        if new_state is not None:
            self.state = new_state
        event = ContainerRunningEvent(self) if self.state == ContainerState.RUNNING else None
        if event is not None:
            self.icond.publish_event(event)

    async def _init_socket(self) -> AsyncTask:
        """ Start client socket and begin serving """
        self.socket_server = await asyncio.start_unix_server(
            self.handle_connection,
            path = self.control_socket_path
        )
        return AsyncTask(self.socket_server.serve_forever, restartable = False)

    async def handle_connection(self, reader, writer):
        """ Handle client connection """
        log.debug('Client connected to %s', self.name)
        reader = MessageReader(reader)
        writer = JSONWriter(writer)
        self.clients += 1
        try:
            msg = await reader.read()
            if isinstance(msg, message.ClientHello):
                await writer.write(msg.create_reply(version = '0.0.1'))
                if self.clients == 1:
                    self.emit_state(ContainerState.RUNNING)
                log.debug('%s: Client handshake completed', self.name)
            else:
                log.error('Invalid handshake message')
                raise message.InvalidMessage(f'Expected ClientHello, got {msg}')
            while True:
                msg = await reader.read()

        except (json.JSONDecodeError, message.InvalidMessage) as e:
            log.error('%s: Error communicating with client: %s', self.name, e)
            self.clients -= 1
            if self.clients == 0:
                # This state change can be redundant if no Hello message was received
                # but it's ok.
                self.emit_state(ContainerState.CONWAITING)

        reader.close()
        writer.close()
        log.error('%s: Client disconnected', self.name)

    async def _run(self):
        # Is it an existing container?
        # TODO: This might be uneccessary if ipo gains some persitent memory over restarts
        d = self.icond.docker
        try:
            container = await d.containers.get(self.container_name)
            log.debug('Container for %s found', self.name)
        except docker.errors.NotFound:
            log.debug('Container for %s not found', self.name)
            # Mount control socket into container
            volumes = {
                self.control_path : {
                    'bind': '/run/icond',
                    'mode': 'ro',
                },
            }
            container = await d.containers.create(self.image,
                                                  name = self.container_name,
                                                  volumes = volumes)
        log.debug(container)
        try:
            await container.start()
        except docker.errors.APIError:
            self.state = ContainerState.FAILED
            self.emit_state()
            return
        # Initializing socket "late"; client API must be able to handle
        # waiting for the appearance of the socket (or re-connecting)
        consrv_task = await self._init_socket()
        self.task_runner.start_task(consrv_task)
        # Now we have to wait for the client to connect
        self.state = ContainerState.CONWAITING
        self.emit_state()
        command_task = AsyncTask(self.inqueue.get)
        self.task_runner.start_task(command_task)
        async for task in self.task_runner.wait_next():
            if self.icond.shutdown:
                break
            if task == command_task:
                command = task.result()
                self.inqueue.task_done()
                if isinstance(command, ShutdownEvent):
                    break
        # Drain queue just in case
        while not self.inqueue.empty():
            self.inqueue.get_nowait()
            self.inqueue.task_done()
        await container.stop()
        # Make sure there aren't any weird left-overs for next run
        self.task_runner.clear()
        self.state = ContainerState.STOPPED
        self.emit_state()
