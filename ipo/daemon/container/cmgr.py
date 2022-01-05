"""
Container manager

Manages starting and stopping of containers
"""
import asyncio
from typing import Union
import re
import logging

from . image import Image
from .container import Container
from .deployment import DeploymentInfo
from . coordinator import (
    ContainerCoordinator,
    RootContainerCoordinator,
)
from .. state import Icond
from ... util.asynctask import AsyncTask, AsyncTaskRunner
from .. events import (
    ShutdownEvent,
)


log = logging.getLogger(__name__)


class ContainerManager:
    """
    Manager of containers.
    """
    ICON_RE = re.compile(r'ICON_\w+')
    deployments: dict[str, ContainerCoordinator]
    tasks: dict[AsyncTask, ContainerCoordinator]
    icond: Icond
    task: Union[None, asyncio.Task]
    inqueue: asyncio.Queue     # Command queue
    task_runner: AsyncTaskRunner

    def __init__(self, icond: Icond):
        """
        icond: Global state
        """
        self.icond = icond
        self.deployments = {}
        self.tasks = {}
        self.inqueue = asyncio.Queue()
        self.task_runner = AsyncTaskRunner()
        self.task = None

    def start(self) -> asyncio.Task:
        """
        Start the container manager service.
        """
        assert self.task is None
        self.icond.eventqueue.listen(ShutdownEvent, self.inqueue)
        self.task = asyncio.create_task(self._run())
        return self.task

    async def _run(self):
        """
        Container manager service main loop
        """
        # TODO: Re-intergrate running containers to ipo. Now just shut them down.
        running_containers = await self.icond.docker.containers.list()
        for container in running_containers:
            if ContainerManager.ICON_RE.match(container.name) is not None:
                log.warning('FIXME: Stopped running unmanaged ICON %s', container.name)
                await container.stop()

        command_task = AsyncTask(self.inqueue.get, restartable = False)
        self.task_runner.start_task(command_task)
        log.debug('ContainerManager started')
        async for task in self.task_runner.wait_next():
            if task == command_task:
                result = task.result()
                self.inqueue.task_done()
                if isinstance(result, ShutdownEvent):
                    log.info('Shutdown event received')
                    break
            elif task in self.tasks:
                # A coordinator quit
                coordinator = self.tasks[task]
                try:
                    # Check if there was an exception; this is the cleanest way
                    # to get a "proper" stack trace logged with logger :(
                    _r = task.result()
                    log.debug('Coordinator %s stopped', coordinator)
                except Exception:
                    log.critical('Exception in coordinator', exc_info = True)
                    # Better remove it alltogether as it's state is probably totally
                    # unpredictable
                    del self.deployments[coordinator.info.image.full_name]

                del self.tasks[task]
            if self.icond.shutdown:
                break

        # TODO: We currently shut down ICONs but this wouldn't strictly necessary - only
        #       some more code to bring back the state of already running when re-starting
        log.info('Shutting down containers...')
        waitfor: list[ContainerCoordinator] = []
        for task, coordinator in self.tasks.items():
            if coordinator.container.is_running():
                await coordinator.stop()
                waitfor.append(task.asynctask)

        self.task_runner.clear()
        # Just extra paranoia that all tasks really have quit when going forward
        if len(waitfor) > 0:
            await asyncio.wait(waitfor)
        log.info('Containers shut-down..')

    async def start_local_icon(self, image_name: str, ports: dict, environment: dict) -> ContainerCoordinator:
        """
        Start a local ICON. The ICON will be the 'root' ICON and the coordination point of
        other instances of this ICON.
        returns: The coordinator for the ICON.
        Throws: ImageException if image_name is invalid.
        """
        image = await Image(image_name)
        return await self.start_local_icon_image(image, ports, environment)

    async def start_local_icon_image(self, image: Image, ports: dict, environment: dict) -> ContainerCoordinator:
        """ Start local icon image """
        # FIXME: Need async at all?
        if image in self.deployments:
            # TODO: Deal with re-starting of containers?
            return self.deployments[image.full_name]

        info = DeploymentInfo(image, ports, environment)
        coordinator = RootContainerCoordinator(info)
        self.deployments[image.full_name] = coordinator
        task = self.task_runner.run(coordinator.run())
        self.tasks[task] = coordinator
        return coordinator

    def list(self) -> list[Container]:
        """ Return all the containers """
        # FIXME: Migrate to coordinator format?
        return list(map(lambda d: d.container,
                        filter(lambda d: d.container is not None,
                               self.deployments.values())))
