"""
ICOND global state
"""
from . asyncdocker import AsyncDockerClient

from . events import ShutdownEvent
from . eventqueue import GlobalEventQueue, Subscription


class Icond:
    """ Icond global state """
    docker: AsyncDockerClient
    shutdown: bool
    eventqueue: GlobalEventQueue

    def __init__(self):
        self.docker = AsyncDockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = GlobalEventQueue()

    def do_shutdown(self):
        """ Shutdown daemon commanded """
        self.shutdown = True
        self.eventqueue.publish(ShutdownEvent())

    def subscribe_event(self, event: type) -> Subscription:
        """ Subscribe to icond events """
        return self.eventqueue.subscribe(event)
