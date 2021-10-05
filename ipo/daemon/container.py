"""
Container controller

Containers are run in docker, but the ICON orchestrator provides more services
to them.
"""
import asyncio
from typing import Union
from enum import Enum
import re
import docker

from . state import Icond
from . asynctask import AsyncTask, AsyncTaskRunner
from . events import (
    ShutdownEvent,
    ContainerRunningEvent,
    ContainerFailedEvent,
)


# Container states
ContainerState = Enum('ContainerState', 'STOPPED STARTING RUNNING FAILED')


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
        return self.task

    def is_running(self):
        """ Is the container in some kind of running state/starting up """
        return not (self.state is ContainerState.STOPPED or self.state is ContainerState.FAILED)

    async def stop(self):
        # FIXME: We don't distinguish between them here, perhaps we should?
        if self.is_running():
            await self.inqueue.put(ShutdownEvent)
        self.task = None

    def emit_state(self):
        """ Send an appropriate event based on the current state """
        event = ContainerRunningEvent(self) if self.state == ContainerState.RUNNING else None
        if event is not None:
            self.icond.publish_event(event)

    async def _run(self):
        # Is it an existing container?
        # TODO: This might be uneccessary if ipo gains some persitent memory over restarts
        d = self.icond.docker
        try:
            container = await d.containers.get(self.container_name)
            print(f'Container for {self.name} found')
        except docker.errors.NotFound:
            print(f'Container for {self.name} not found')
            container = await d.containers.create(self.image,
                                                  name = self.container_name,
                                                  detach = True)
        print(container)
        try:
            await container.start()
        except docker.errors.APIError:
            self.state = ContainerState.FAILED
            self.emit_state()
            return
        self.state = ContainerState.RUNNING
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
        self.state = ContainerState.STOPPED
        self.emit_state()


class ContainerManager:
    """
    Manager of containers.
    """
    ICON_RE = re.compile(r'ICON_\w+')
    containers: dict[str, Container]  # List of containers
    task_container: dict[AsyncTask, Container]
    icond: Icond
    task: Union[None, asyncio.Task]
    inqueue: asyncio.Queue()   # Command queue
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
                print(f'FIXME: Stopped running unmanaged ICON {container.name}')
                await container.stop()

        command_task = AsyncTask(self.inqueue.get, restartable = False)
        self.task_runner.start_task(command_task)
        print('ContainerManager started')
        async for task in self.task_runner.wait_next():
            e = task.exception()
            if e is not None:
                print(f'Task {task} had an exception {e}')
            elif task == command_task:
                result = task.result()
                self.inqueue.task_done()
                if isinstance(result, ShutdownEvent):
                    print('Shutdown event received')
                    break
            elif task in self.task_container:
                # A container died
                container = self.task_container[task]
                print(f'Container {container.name} died')
                del self.task_container[task]
            if self.icond.shutdown:
                break

        # TODO: We currently shut down ICONs but this wouldn't strictly necessary - only
        #       some more code to bring back the state of already running when re-starting
        print('Shutting down containers...')
        waitfor = list()
        for container in self.containers.values():
            if container.task and not container.task.done():
                waitfor.append(container.task)
            await container.stop()
        if len(waitfor) > 0:
            await asyncio.wait(waitfor)
        print('Containers shut-down..')

    async def run_container(self, image) -> Container:
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
            container = Container(image, image, self.icond)  # TODO: Allow multiple ICONs from same image

        task = AsyncTask(lambda: container.start(), restartable = False)
        self.task_container[task] = container
        self.task_runner.start_task(task)
        self.containers[image] = container
        return container

    async def list(self) -> list[Container]:
        """ Return all the containers """
        return list(self.containers.values())
