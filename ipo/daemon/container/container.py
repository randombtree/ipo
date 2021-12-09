"""
Container controller

Containers are run in docker, but the ICON orchestrator provides more services
to them.
"""
import asyncio
from typing import Optional
from collections.abc import Iterator
from enum import Enum
import os.path
import socket
import itertools
import logging

import docker  # type: ignore
from docker.models.images import (  # type: ignore
    Image as DockerImage
)

from .. state import Icond
from ... util.asynctask import AsyncTaskRunner
from .. events import (
    ShutdownEvent,
    ContainerRunningEvent,
    ContainerFailedEvent
)
from ...api import message
from ..messagetask import MessageTaskDispatcher, get_message_handlers
from . import containertask


log = logging.getLogger(__name__)


# Container states
ContainerState = Enum('ContainerState', 'STOPPED STARTING CONWAITING RUNNING FAILED')


def container_base_name(image_name: str) -> str:
    """
    Generate some kind of simple human readable name for image,
    discarding paths and versions.
    """
    *_path, name_version = image_name.split('/')
    return name_version.split(':')[0]


def container_name_generator(image: DockerImage) -> Iterator[str]:
    """ Generate new name possibilities for container """
    base_name = container_base_name(image.attrs['RepoTags'][0])
    container_name = f'ICON_{base_name}'

    yield container_name
    _m, sid = image.short_id.split(':')
    container_name += f'-{sid}'
    yield container_name
    for ndx in itertools.count(start = 1):
        yield f'{container_name}:{ndx}'


async def is_local_image(image: str) -> bool:
    """ Quick heuristics to check if image is a local instance or a remote one """
    first, *_rest = image.split('/')
    addrport = first.split(':')
    if len(addrport) != 2:
        return True
    host, port = addrport
    if not port.isdecimal():
        return True
    try:
        # Throws if this is not a correct one
        await asyncio.get_event_loop().getaddrinfo(host, port)
    except socket.gaierror:
        return True
    # It's a remote repository
    return False


class Container:
    """ A running container we are providing ICON services to """
    image: str
    icond: Icond
    params: dict[str, str]
    state: ContainerState
    inqueue: asyncio.Queue
    container_name: Optional[str]
    control_path: Optional[str]  # Path to mount into container
    clients: int                 # Connected clients count

    def __init__(self, image: str, icond: Icond, **params):
        """
        name: Container name
        params: Container parameters for docker
        icond: Daemon global state
        """
        self.image = image
        self.icond = icond
        self.params = params
        self.state = ContainerState.STOPPED
        self.inqueue = asyncio.Queue()
        self.container_name = None  # Figured out when starting
        self.control_path = None    # Ditto
        self.icond.eventqueue.listen(ShutdownEvent, self.inqueue)
        self.clients = 0

    async def run(self):
        """ Run container """
        assert not self.is_running()
        self.emit_state(ContainerState.STARTING)
        await self._run()

    def is_running(self):
        """ Is the container in some kind of running state/starting up """
        return not (self.state is ContainerState.STOPPED or self.state is ContainerState.FAILED)

    async def stop(self):
        """ Stop container """
        # FIXME: We don't distinguish between them here, perhaps we should?
        if self.is_running():
            await self.inqueue.put(ShutdownEvent)

    def _state_event(self):
        if self.state == ContainerState.RUNNING:
            return ContainerRunningEvent(self)
        if self.state == ContainerState.FAILED:
            return ContainerFailedEvent(self)
        return None

    def emit_state(self, new_state = None):
        """ Send an appropriate event based on the current state """
        if new_state is not None:
            self.state = new_state
        event = self._state_event()
        if event is not None:
            self.icond.publish_event(event)

    async def connection_server(self):
        """ Server for incoming connectio(s) from container """
        socket_name = f'{self.control_path}/icon.sock'
        # Clean up stale socket
        if os.path.exists(socket_name):
            os.unlink(socket_name)  # If this throws we are screwed anyway
        log.debug('Start serving container at %s', socket_name)
        socket_server = await asyncio.start_unix_server(
            self.handle_connection,
            path = socket_name,
        )
        await socket_server.serve_forever()

    async def handle_connection(self, reader, writer):
        """ Handle client connection """
        log.debug('Client connected to %s', self.image)
        hello_received = False
        async with MessageTaskDispatcher(reader, writer, {}, self.icond, container = self) as dispatcher:
            async for unhandled, outqueue in dispatcher:
                if not hello_received:
                    if isinstance(unhandled, message.ClientHello):
                        msg = unhandled
                        hello_received = True
                        log.debug('%s: Client handshake completed', self.image)
                        await outqueue.put(msg.create_reply(version = '0.0.1'))
                        self.clients += 1
                        if self.clients == 1:
                            self.emit_state(ContainerState.RUNNING)
                        # Allow other messages to be handled after handshake
                        dispatcher.add_handlers(get_message_handlers(containertask))
                    else:
                        log.error('%s: Invalid handshake message %s', self.image, unhandled)
                        break
                elif isinstance(unhandled, message.IconMessage):
                    # There is no pretty way to go about this, just disconnect
                    log.warning('Invalid message %s', unhandled)
                    break
                else:
                    e = unhandled.exception()
                    log.error('Task %s died? %s', unhandled, e)
                    break
        if hello_received:
            self.clients -= 1
            if self.clients == 0:
                log.debug('This was the last client exiting')
                self.emit_state(ContainerState.CONWAITING)

        log.debug('Client disconnected')

    def _init_control_path(self):
        """ Initialize control path for new container """
        control_path = f'{self.icond.config.run_directory}/{self.container_name}'
        if not os.path.exists(control_path):
            os.mkdir(control_path, mode = 0o700)
        # FIXME?: /else clean up all files there perhaps?
        self.control_path = control_path

    async def _start_container(self, image: DockerImage):
        """ Start container """
        # Is it an existing container?
        # TODO: This might be uneccessary if ipo gains some persitent memory over restarts
        d = self.icond.docker
        try:
            for container_name in container_name_generator(image):
                self.container_name = container_name
                container = await d.containers.get(container_name)
                log.debug('Possible container %s for %s found', container_name, self.image)
                # Check that images match
                # TODO: Support updating long lived container with newer image
                if container.image == image:
                    break

            # Fixme: Hmph, the current ICON api is a bit hacky, ideally we would
            #        mirror docker api, e.g. first create container then specify
            #        to run it
            if self.params:
                log.warning('Re-using container, parameters ignored')
            self._init_control_path()
        except docker.errors.NotFound:
            log.debug('Container for %s not found, starting new', self.image)
            # The last container_name not to exist
            self._init_control_path()
            # Mount control socket into container
            volumes = {
                self.control_path : {
                    'bind': '/run/icond',
                    'mode': 'ro',
                },
            }
            container = await d.containers.create(self.image,
                                                  name = self.container_name,
                                                  volumes = volumes,
                                                  **self.params)
        log.debug(container)
        try:
            await container.start()
        except docker.errors.APIError:
            # It failed to start, probably something wrong in the image, no need to keep
            # the failed container around
            await container.remove()
            self.state = ContainerState.FAILED
            self.emit_state()
            return
        return container

    async def _publish_image(self):
        """
        Make sure this image is available in local repository
        returns: The docker image discovered
        """
        d = self.icond.docker
        image = await d.images.get(self.image)
        repotag = f'localhost:5000/ipo/local/{self.image}:latest'

        def has_tag() -> bool:
            for tag in image.tags:
                if tag == repotag:
                    return True
            return False

        if not has_tag():
            await image.tag(repotag)

        await d.images.push(repotag)
        return image

    async def _get_image(self):
        """ Fetch image from remote repository if not available locally """
        d = self.icond.docker
        log.debug('Pulling image %s from repository', self.image)
        image =  await d.images.pull(self.image)
        # Curiously, this seems to return a list even when not
        # specifying to fetch all tags.
        if isinstance(image, list):
            return image[0]
        # If the API changes to reflect the documentation some day
        return image

    async def _is_container_running(self, container):
        """ Check regularly if the container is actually up """
        while True:
            await container.reload()
            if container.status != 'running':
                log.debug('%s: Container status changed to %s', self.image, container.status)
                break
            await asyncio.sleep(60)

    async def _handle_tasks(self, container):
        """ Wait for tasks """
        task_runner = AsyncTaskRunner()

        # Initializing socket "late"; client API must be able to handle
        # waiting for the appearance of the socket (or re-connecting)
        consrv_task = task_runner.run(self.connection_server())
        command_task = task_runner.run(self.inqueue.get)
        container_running_task = task_runner.run(self._is_container_running(container))

        async for task in task_runner.wait_next():
            if self.icond.shutdown:
                break
            if task == command_task:
                command = task.result()
                self.inqueue.task_done()
                if isinstance(command, ShutdownEvent):
                    break
            elif task == consrv_task:
                log.error('Failed to start connection server, aboring')
                break
            elif task == container_running_task:
                log.info('%s exited', self.image)
                break

        # Make sure there aren't any weird left-overs for next run
        task_runner.clear()

    async def _run(self):
        try:
            if await is_local_image(self.image):
                # Local image must be available in local repo for
                # migration to happen
                image = await self._publish_image()
            else:
                # We need to get/refresh the image from remote repo
                image = await self._get_image()
        except docker.errors.ImageNotFound as e:
            log.warning('Image not found %s', e)
            self.emit_state(ContainerState.FAILED)
            return
        except (docker.errors.APIError, docker.errors.InvalidRepository):
            # Image URL was wrong or badly formatted?
            log.warning('Invalid image %s?', self.image, exc_info = True)
            self.emit_state(ContainerState.FAILED)
            return

        # Now we should have a valid image

        container = await self._start_container(image)
        if container is None:
            log.error('Failed to start container')
            return

        # Now we have to wait for the client to connect
        self.state = ContainerState.CONWAITING
        self.emit_state()

        await self._handle_tasks(container)

        # Drain queue just in case
        while not self.inqueue.empty():
            self.inqueue.get_nowait()
            self.inqueue.task_done()
        # We don't remove the container by default, allowing hot re-starts of ICONs
        # (The container yard management sw should prune stale containers regularly)
        await container.stop()
        self.state = ContainerState.STOPPED
        self.emit_state()

    def __str__(self):
        return f'<Container: {self.image}>'
