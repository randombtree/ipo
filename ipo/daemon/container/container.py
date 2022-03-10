"""
Container controller

Containers are run in docker, but the ICON orchestrator provides more services
to them.
"""
import asyncio
from typing import Optional
from collections.abc import Iterator
import os.path
import socket
import itertools
import logging

import docker  # type: ignore
from docker.models.containers import (  # type: ignore
    Container as DockerContainer
)

from .. import state
from ...util.signal import Signal, Emitter

from ... util.asynctask import AsyncTaskRunner
from .. events import (
    ShutdownEvent,
    ContainerRunningEvent,
    ContainerFailedEvent
)
from ...api import message
from ..messagetask import MessageTaskDispatcher, get_message_handlers
from . import containertask
from . deployment import DeploymentInfo, DeploymentCoordinator, DeploymentState as ContainerState
from . image import Image


log = logging.getLogger(__name__)


def container_base_name(image_name: str) -> str:
    """
    Generate some kind of simple human readable name for image,
    discarding paths and versions.
    """
    *_path, name_version = image_name.split('/')
    return name_version.split(':')[0]


def container_name_generator(image: Image) -> Iterator[str]:
    """ Generate new name possibilities for container """
    container_name = f'ICON_{image.image_name}'

    yield container_name
    # If the simple name isn't available, try appending the short id from image.
    _m, sid = image.docker_image.short_id.split(':')
    container_name += f'-{sid}'
    yield container_name
    # And if that fails, start numbering
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


class Container(Emitter):
    """ A running container we are providing ICON services to """
    StateChanged = Signal()      # State change signal, called with state = 'new state'

    deployment: DeploymentInfo
    coordinator: DeploymentCoordinator
    state: ContainerState
    inqueue: asyncio.Queue
    container_name: Optional[str]
    control_path: Optional[str]  # Path to mount into container
    clients: int                 # Connected clients count

    def __init__(self, deployment: DeploymentInfo, coordinator: DeploymentCoordinator):
        """
        deployment: Info on deployment
        """
        self.deployment = deployment
        self.coordinator = coordinator
        self.state = ContainerState.STOPPED
        self.inqueue = asyncio.Queue()
        self.container_name = None  # Figured out when starting
        self.control_path = None    # Ditto
        state.Icond.instance().eventqueue.listen(ShutdownEvent, self.inqueue)
        self.clients = 0

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

    async def emit_state(self, new_state = None):
        """ Send an appropriate event based on the current state """
        old_state = self.state
        if new_state is not None:
            self.state = new_state
        if old_state == new_state:
            return
        await self.StateChanged(state = new_state)
        event = self._state_event()
        if event is not None:
            state.Icond.instance().publish_event(event)

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
        log.debug('Client connected to %s', self.container_name)
        hello_received = False
        async with MessageTaskDispatcher(reader, writer, {}, state.Icond.instance(), container = self) as dispatcher:
            async for unhandled, outqueue in dispatcher:
                if not hello_received:
                    if isinstance(unhandled, message.ClientHello):
                        msg = unhandled
                        hello_received = True
                        log.debug('%s: Client handshake completed', self.container_name)
                        await outqueue.put(msg.create_reply(version = '0.0.1'))
                        self.clients += 1
                        if self.clients == 1:
                            await self.emit_state(ContainerState.RUNNING)
                        # Allow other messages to be handled after handshake
                        dispatcher.add_handlers(get_message_handlers(containertask))
                    else:
                        log.error('%s: Invalid handshake message %s', self.container_name, unhandled)
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
                await self.emit_state(ContainerState.CONWAITING)

        log.debug('Client disconnected')

    def _init_control_path(self):
        """ Initialize control path for new container """
        config = state.Icond.instance().config
        control_path = f'{config.run_directory}/{self.container_name}'
        if not os.path.exists(control_path):
            os.mkdir(control_path, mode = 0o700)
        # FIXME?: /else clean up all files there perhaps?
        self.control_path = control_path

    def _valid_container(self, container: DockerContainer) -> bool:
        """
        Check if the docker container matches our parameters.
        - Ports and environment variables can change between deployments
          even if the image stays the same.
        """
        docker_image = self.deployment.image.docker_image
        # Check that images match
        if container.image != docker_image:
            return False
        # Check that container matches
        # It's possible that the origin host changes some mappings
        # so best be overly cautious
        ports = container.attrs['NetworkSettings']['Ports']
        for port, hostport in self.deployment.ports.items():
            # Port is in docker format, e.g. '8080/tcp'
            if port in ports:
                # Does the container use dynamic ports?
                # In that case, any existing mapping can be re-used
                # (we assume it's "sane")
                if hostport is None:
                    continue
                # docker port config looks something like this:
                # {'8080/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '8080'}, ..]
                for hostmap in ports[port]:
                    # We assume any matching port is ok
                    # and IPv4 mapping matches IPv6
                    if int(hostmap['HostPort']) == hostport:
                        break
                    log.debug('%s: HostPort mapping differs on container %s',
                              self.deployment.image,
                              container.name)
                    return False
        # Check environment also
        # Weird that dockers env isn't in a dictionary.. make it so
        docker_env: list[str] = container.attrs['Config']['Env']
        env: dict[str, str] = dict(map(lambda v: tuple(v.split('=')[:2]),  # type: ignore
                                       filter(lambda v: '=' in v[1:], docker_env)))  # /paranoid
        for envvar, val in self.deployment.environment.items():
            if envvar not in env or env[envvar] != val:
                log.debug('Environment %s mismatches, %s != %s',
                          envvar, val, env[envvar])
                return False
        return True

    def _update_ports(self, container: DockerContainer):
        """ Update dynamic ports to deployment info """
        # Docker container port map will contain mappings like this
        # {'8080/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '8080'}, {'HostIp': '::', 'HostPort': '8080'}]}
        # We have it in a simpler form as we always assume IPv4/6 have the same mappings
        # e.g. {'8080/tcp': 8080 }, note conversion to int port numbering!
        log.debug('Original port mappings %s', self.deployment.ports)
        self.deployment.ports = dict(map(lambda pmap: (pmap[0], int(pmap[1][0]['HostPort'])),
                                         container.attrs['NetworkSettings']['Ports'].items()))
        log.debug('Updated port mappings %s', self.deployment.ports)

    async def _start_container(self):
        """ Start container """
        # Is it an existing container?
        # TODO: This might be uneccessary if ipo gains some persitent memory over restarts
        d = state.Icond.instance().docker
        try:
            for container_name in container_name_generator(self.deployment.image):
                self.container_name = container_name
                container = await d.containers.get(container_name)
                log.debug('Possible container %s for %s found',
                          container_name,
                          self.deployment.image)
                if self._valid_container(container):
                    break

            self._init_control_path()
        except docker.errors.NotFound:
            log.debug('Container for %s not found, starting new', self.deployment.image)
            # The last container_name not to exist
            self._init_control_path()
            # Mount control socket into container
            volumes = {
                self.control_path : {
                    'bind': '/run/icond',
                    'mode': 'ro',
                },
            }
            log.debug('ports = %s, environment = %s', self.deployment.ports, self.deployment.environment)
            container = await d.containers.create(self.deployment.image.full_name,
                                                  name = self.container_name,
                                                  volumes = volumes,
                                                  environment = self.deployment.environment,
                                                  ports = self.deployment.ports)
        log.debug(container)
        try:
            await container.start()
            await container.reload()   # Funny enough, start() doesn't update the container object :)
        except docker.errors.APIError:
            # It failed to start, probably something wrong in the image, no need to keep
            # the failed container around
            await container.remove()
            await self.emit_state(ContainerState.FAILED)
            return
        # Update dynamic ports
        self._update_ports(container)
        return container

    async def _is_container_running(self, container):
        """ Check regularly if the container is actually up """
        while True:
            await container.reload()
            if container.status != 'running':
                log.debug('%s: Container status changed to %s',
                          self.deployment.image,
                          container.status)
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
            if task == command_task:
                command = task.result()
                self.inqueue.task_done()
                if isinstance(command, ShutdownEvent):
                    break
            elif task == consrv_task:
                log.error('Failed to start connection server, aboring')
                break
            elif task == container_running_task:
                log.info('%s/%s exited', self.container_name, self.deployment.image)
                break

        # Make sure there aren't any weird left-overs for next run
        task_runner.clear()

    async def run(self):
        """ Run container """
        assert not self.is_running()
        await self.emit_state(ContainerState.STARTING)

        container = await self._start_container()
        if container is None:
            log.error('Failed to start container')
            return

        # Now we have to wait for the client to connect
        await self.emit_state(ContainerState.CONWAITING)

        await self._handle_tasks(container)

        # Drain queue just in case
        while not self.inqueue.empty():
            self.inqueue.get_nowait()
            self.inqueue.task_done()
        # We don't remove the container by default, allowing hot re-starts of ICONs
        # (The container yard management sw should prune stale containers regularly)
        await container.stop()
        await self.emit_state(ContainerState.STOPPED)

    def __str__(self):
        return f'<Container {self.container_name} for {self.deployment.image}>'
