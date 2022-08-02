"""
ICOND global state
"""
import asyncio
import socket
import logging
from typing import Optional

from .. util.asynctask import AsyncTaskRunner
from . asyncdocker import AsyncDockerClient

from . events import ShutdownEvent
from . eventqueue import EventQueue, Subscription
from . config import DaemonConfig
from . routing import RouteManager

log = logging.getLogger(__name__)


class Icond:
    """ Icond global state """
    _instance: Optional['Icond'] = None            # Singleton
    docker: AsyncDockerClient
    shutdown: bool
    eventqueue: EventQueue
    config: DaemonConfig
    router: RouteManager

    def __init__(self):
        # Work around circular deps
        from . import container  # pylint: disable=import-outside-toplevel
        from . import control    # pylint: disable=import-outside-toplevel
        from . import orchestrator  # pylint: disable=import-outside-toplevel

        self.docker = AsyncDockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = EventQueue()
        self.config = DaemonConfig()
        self.cmgr = container.ContainerManager(self)
        self.router = RouteManager(self.config)
        self.ctrl = control.ControlServer(self)
        self.orchestrator = orchestrator.OrchestratorManager(self)
        Icond._instance = self

    @classmethod
    def instance(cls) -> 'Icond':
        """ Return Icond instance """
        if cls._instance is None:
            raise Exception('Icond hasn\'t been initialized?')
        return cls._instance

    async def shutdown_waiter(self):
        """ Helper: Quits (returns) on shutdown event """
        with self.subscribe_event(ShutdownEvent) as shutdown_event:
            await shutdown_event.get()

    async def run(self):
        """ Run the sub-modules of the daemon """
        runner = AsyncTaskRunner()
        await self.router.start()
        ctrl_task = runner.run(self.ctrl.run())
        shutdown_task = runner.run(self.shutdown_waiter())
        cmgr_task = runner.run(self.cmgr.run())
        orch_task = runner.run(self.orchestrator.run())
        # For debug purposes when something fails
        task_map = {
            ctrl_task: 'Controller service',
            cmgr_task: 'Container manager service',
            orch_task: 'Orchestrator manager service'
        }

        async for task in runner:
            try:
                _r = task.result()
                if shutdown_task == task:
                    log.info('Shutdown signaled. Quitting..')
                    await self.router.stop()
                    log.info('Waiting for tasks to shut down...')
                    await asyncio.wait({cmgr_task.asynctask,
                                        self.router.task,
                                        orch_task.asynctask,
                                        })
                    break
                if not self.shutdown:
                    log.error('Unexpected exit of task %s (%s). Quitting!',
                              task,
                              task_map[task] if task in task_map else 'Unknown?')
                    self.do_shutdown()
            except Exception as e:
                log.critical('Exception in service', exc_info=True)
                raise e

        runner.clear()
        # Also, leaving docker session open will spew warnings
        await self.docker.close()

    def do_shutdown(self):
        """ Shutdown daemon commanded """
        log.debug('Shutdown called')
        self.shutdown = True
        self.eventqueue.publish(ShutdownEvent())

    def subscribe_event(self, event: type) -> Subscription:
        """ Subscribe to icond events """
        return self.eventqueue.subscribe(event)

    def publish_event(self, event):
        """ Send a global event to all subscribers for the event type """
        self.eventqueue.publish(event)

    def get_ip_address(self) -> str:
        """ Returns the IP address of this orchestrator """
        # TODO: A bit hacky atm; Should only be called after the router
        #       has determined our IP address - and currently it relies on
        #       DHT replies.
        addr = self.router.get_current_ip()
        if addr is not None:
            return socket.inet_ntoa(addr)
        log.error('IP address not available')
        return '0.0.0.0'
