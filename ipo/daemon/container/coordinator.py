"""
Container coordinators.

Glue between remote and local containers.
"""
from asyncio import Queue
import logging
from abc import ABCMeta

from typing import Optional, Any, TypeVar

from ... api import message
from ...util.signal import Event, Emitter
from .. import state
from .. messagetask import MessageTaskHandler
from .container import Container
from .deployment import (
    DeploymentInfo,
    DeploymentCoordinator,
    DeploymentState,
    RemoteDeployment,
)
from ... util.asynctask import AsyncTaskRunner, AsyncTask
from .. events import (
    ShutdownEvent,
    MessageEvent,
)
from ... misc.types import Hostaddr


log = logging.getLogger(__name__)


class ContainerCoordinator(DeploymentCoordinator, Emitter, metaclass = ABCMeta):
    """ Container Deployment glue """
    info: DeploymentInfo
    runner: AsyncTaskRunner
    inqueue: Queue
    container: Container
    deployments: dict[Hostaddr, RemoteDeployment]

    def __init__(self, info: DeploymentInfo):
        self.info = info
        self.runner = AsyncTaskRunner()
        self.inqueue = Queue()
        self.container = Container(self.info, self)
        self.container.StateChanged.connect(self.inqueue)
        self.deployments = {}

    @classmethod
    def create(cls: type['CoordinatorType'], info: DeploymentInfo) -> 'CoordinatorType':
        """ Create a container coordinator """
        return cls(info)

    async def stop(self):
        """ Stop the coordinator """
        if self.container is not None and self.container.is_running():
            await self.inqueue.put(ShutdownEvent)
            await self.inqueue.join()

    async def run(self):
        """
        Run a container based on info provided.

        Returns if ordered to quit or with exception if container coudln't be instantiated
        """
        container_task = self.runner.run(self.container.run())
        command_task = self.runner.run(self.inqueue.get)
        pending_shutdown = False  # For waiting gracefully for shutdown
        async for task in self.runner:
            if command_task == task:
                result = task.result()

                if isinstance(result, ShutdownEvent):
                    # From stop()
                    pending_shutdown = True
                    await self.container.stop()
                    # TODO: Add timeout task if container refuses to shut down
                elif isinstance(result, Event):
                    # Container signal
                    event = result
                    if event.signal == self.container.StateChanged:
                        log.debug('Container state change %s', event)
                        # TODO: Need to do anything?
                else:
                    self.handle_command(result)
            elif container_task == task:
                if pending_shutdown:
                    log.info('%s shut down successfully', self.container)
                    self.inqueue.task_done()  # Wake up .stop() waiting in join()
                else:
                    log.warning('%s shut down unexpectedly', self.container)
                    # TODO: Notify etc.
                break
            else:
                await self.handle_task(task)

    def __str__(self):
        return f'<Coordinator {self.container}>'

    # disable warning on missing self use
    # pylint: disable=R0201
    async def handle_task(self, task: AsyncTask):
        """ Hook for child class to handle it's own tasks """
        log.error('Unhandled task %s', task)

    async def handle_command(self, command: Any):
        """ Hook for child class to handle incoming command """
        log.error('Unhandled command %s', command)


# Type constraint for different ContainerCoordinators
CoordinatorType = TypeVar('CoordinatorType', bound = ContainerCoordinator)


class RemoteDeploymentMonitor(RemoteDeployment, MessageTaskHandler):
    """
    Connection from local coordinator to remote deployment
    """
    info: DeploymentInfo
    saved_state: Optional[DeploymentState]   # Out-of-order initialization guard for state changes

    def __init__(self, outqueue: Queue, icond: state.Icond, **params):
        self.info = params['info']
        del params['info']
        RemoteDeployment.__init__(self)
        MessageTaskHandler.__init__(self, outqueue, icond, **params)

        self.saved_state  = None

    def is_starting(self):
        """ Check if the container is still in starting state """
        return self.remote_ip is None

    async def handler(self, initial_msg: message.IconMessage):
        # initial_msg contains our sessid
        # Currently we can only request container ports, which will be assigned to host addresses on remote and
        # returned in the reply message
        ports = list(self.info.ports.keys())
        log.debug('Starting migration ports = %s, environment = %s',
                  self.info.ports, self.info.environment)
        msg: message.IconMessage = message.MigrationRequest.reply_to(
            initial_msg,
            image_name = self.info.image.full_name,
            ports = ports,
            environment = self.info.environment)
        await self._send(msg)
        while True:
            async with self as event:
                log.debug('Received event: %s', msg)
                if isinstance(event, ShutdownEvent):
                    log.debug('Shutting down')
                    await self._send(message.ShutdownCommand.reply_to(initial_msg))
                    # No need to wait.. remote will shut down container later due to missing uplink
                    return
                assert isinstance(event, MessageEvent)
                msg = event.msg
                if message.MigrationResponse.match(msg):
                    if self.is_starting():
                        # TODO: Validate some more?
                        # MSG validator should take out the obvious ones, but what if the port mappings are all wrong?
                        self.remote_ports = msg.ports
                        self.remote_ip = msg.ip
                        # Paranoid: Just in case running state arrives of some reason arrives before us
                        if self.saved_state is not None:
                            log.debug('Applying out-of-order state')
                            await self.set_state(self.saved_state)
                            self.saved_state = None
                        # State changes will flow in so that possible migrators can check the state.
                    else:
                        log.warning('Migration response when up and running')
                elif message.DeploymentStateChanged.match(msg):
                    state_str = msg.state
                    if state_str not in DeploymentState.__members__:
                        log.warning('Invalid state from remote: %s', state_str)
                    else:
                        new_state = DeploymentState.__members__[state_str]
                        if self.is_starting() and new_state not in [DeploymentState.STOPPED, DeploymentState.STARTING, DeploymentState.FAILED]:
                            # Paranoid: Catch out-of-order if it happens
                            self.saved_state = new_state
                            log.debug('Out-of-order state %s when deploying', new_state)
                        else:
                            await self.set_state(new_state)
                        # TODO: Handle Failed etc.


class RootDeploymentCoordinator(ContainerCoordinator):
    """
    Deployment coordinator in the source/root orchestrator.
    """

    # async def handle_command(self, command: Any):
    #     super().handle_command(command)

    async def migrate_to(self, ip: str, port: int) -> RemoteDeployment:
        om = state.Icond.instance().orchestrator
        hostaddr: Hostaddr = (ip, port)
        if hostaddr in self.deployments:
            log.debug('Using existing deployment')
            # TODO: Handle failed deployment
            return self.deployments[hostaddr]
        log.debug('Getting orchestrator %s:%d...', ip, port)
        orchestrator = await om.get_orchestrator((ip, port))
        log.debug('Waiting for connection...')
        await orchestrator.wait_until_connected()
        log.debug('Start deploying...')
        deployment = await orchestrator.start_session(RemoteDeploymentMonitor, info = self.info)
        return deployment


class ForeignDeploymentCoordinator(ContainerCoordinator):

    """
    This is the coordinator for running a foreign container.
    Essentially the glue between the running container and remote orchestrator/root coordinator.
    """
    async def migrate_to(self, ip: str, port: int) -> RemoteDeployment:
        # TODO: Contact root coordinator and handle coordination from there?
        raise Exception('Foreign migration request not implemented')
