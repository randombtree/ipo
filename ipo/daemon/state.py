"""
ICOND global state
"""
import asyncio
import logging

from .. util.asynctask import AsyncTaskRunner
from . asyncdocker import AsyncDockerClient

from . events import ShutdownEvent
from . eventqueue import EventQueue, Subscription
from . config import DaemonConfig
from . routing import RouteManager

log = logging.getLogger(__name__)


class Icond:
    """ Icond global state """
    docker: AsyncDockerClient
    shutdown: bool
    eventqueue: EventQueue
    config: DaemonConfig
    router: RouteManager

    def __init__(self):
        # Work around circular deps
        from . import container  # pylint: disable=import-outside-toplevel
        from . import control    # pylint: disable=import-outside-toplevel

        self.docker = AsyncDockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = EventQueue()
        self.config = DaemonConfig()
        self.cmgr = container.ContainerManager(self)
        self.router = RouteManager(self.config)
        self.ctrl = control.ControlServer(self)

    async def _shutdown_waiter(self):
        with self.subscribe_event(ShutdownEvent) as shutdown_event:
            await shutdown_event.get()

    async def run(self):
        """ Run the sub-modules of the daemon """
        runner = AsyncTaskRunner()
        await self.router.start()
        runner.run(self.ctrl.run())
        shutdown_task = runner.run(self._shutdown_waiter())
        cmgr_task = runner.run(lambda: self.cmgr.start())

        try:
            async for task in runner:
                _r = task.result()
                if shutdown_task == task:
                    log.info('Shutdown signaled. Quitting..')
                    break
                log.error('Unexpected exit of task %s. Quitting!', task)
                self.do_shutdown()
        finally:
            runner.clear()
            await self.router.stop()
            log.info('Waiting for tasks to shut down...')
            await asyncio.wait({cmgr_task.asynctask, self.router.task})
            # Also, leaving docker session open will spew warnings
            await self.docker.close()

    def do_shutdown(self):
        """ Shutdown daemon commanded """
        self.shutdown = True
        self.eventqueue.publish(ShutdownEvent())

    def subscribe_event(self, event: type) -> Subscription:
        """ Subscribe to icond events """
        return self.eventqueue.subscribe(event)

    def publish_event(self, event):
        """ Send a global event to all subscribers for the event type """
        self.eventqueue.publish(event)
