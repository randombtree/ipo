"""
Container manager

Manages starting and stopping of containers
"""
import asyncio
from typing import Union
import re
import logging

from .container import Container
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
    containers: dict[str, Container]  # List of containers
    task_container: dict[AsyncTask, Container]
    icond: Icond
    task: Union[None, asyncio.Task]
    inqueue: asyncio.Queue     # Command queue
    task_runner: AsyncTaskRunner

    def __init__(self, icond: Icond):
        """
        icond: Global state
        """
        self.icond = icond
        self.containers = dict()
        self.task_container = dict()
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
            elif task in self.task_container:
                # A container died
                container = self.task_container[task]
                try:
                    # Check if there was an exception; this is the cleanest way
                    # to get a "proper" stack trace logged with logger :(
                    r = task.result()
                    log.debug('Container %s stopped', container)
                except Exception:
                    log.critical('Exception in container handler', exc_info = True)
                    # Better remove it alltogether as it's state is probably totally
                    # unpredictable
                    del self.containers[container.image]

                del self.task_container[task]
            if self.icond.shutdown:
                break

        # TODO: We currently shut down ICONs but this wouldn't strictly necessary - only
        #       some more code to bring back the state of already running when re-starting
        log.info('Shutting down containers...')
        waitfor = list()
        for container in self.containers.values():
            if container.task and not container.task.done():
                waitfor.append(container.task)
            await container.stop()
        if len(waitfor) > 0:
            await asyncio.wait(waitfor)
        log.info('Containers shut-down..')

    async def run_container(self, image, **params) -> Container:
        """
        Run (start) the container.
        image: The image to use
        """
        # FIXME: Later on the image can contain a source repo
        if image in self.containers:
            container = self.containers[image]
            if container.is_running():
                return container
        else:
            # TODO: Allow multiple ICONs from same image
            container = Container(image, self.icond, **params)

        task = self.task_runner.run(container.run())
        self.task_container[task] = container
        self.containers[image] = container
        return container

    def list(self) -> list[Container]:
        """ Return all the containers """
        return list(self.containers.values())
