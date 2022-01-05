"""
Container coordinators.

Glue between remote and local containers.
"""
from asyncio import Queue
import logging
from abc import ABCMeta

from .container import Container
from .deployment import DeploymentInfo, DeploymentCoordinator
from ... util.asynctask import AsyncTaskRunner
from .. events import (
    ShutdownEvent,
)


log = logging.getLogger(__name__)


class ContainerCoordinator(DeploymentCoordinator, metaclass = ABCMeta):
    """ Container Deployment glue """
    info: DeploymentInfo
    runner: AsyncTaskRunner
    inqueue: Queue
    container: Container

    def __init__(self, info: DeploymentInfo):
        self.info = info
        self.runner = AsyncTaskRunner()
        self.inqueue = Queue()
        self.container = Container(self.info, self)

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
        pending_shutdown = False
        async for task in self.runner:
            if command_task == task:
                result = task.result()
                pending_shutdown = True

                if isinstance(result, ShutdownEvent):
                    await self.container.stop()
            elif container_task == task:
                if pending_shutdown:
                    log.info('%s shut down successfully', self.container)
                    self.inqueue.task_done()  # Wake up .stop() waiting in join()
                else:
                    log.warning('%s shut down unexpectedly', self.container)
                    # TODO: Notify etc.
                break

    def __str__(self):
        return f'<Coordinator {self.container}>'


class RootContainerCoordinator(ContainerCoordinator):
    """
    Deployment coordinator in the source/root orchestrator.
    """

    async def migrate_to(self, ip: str, port: int):
        ...


class RemoteContainerCoordinator(ContainerCoordinator):
    """
    Glue between container and remote orchestrator/root coordinator.
    """
    async def migrate_to(self, ip: str, port: int):
        ...
