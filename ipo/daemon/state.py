"""
ICOND global state
"""
from . asyncdocker import AsyncDockerClient

from . events import ShutdownEvent
from . eventqueue import EventQueue, Subscription
from . config import DaemonConfig


class Icond:
    """ Icond global state """
    docker: AsyncDockerClient
    shutdown: bool
    eventqueue: EventQueue
    config: DaemonConfig

    def __init__(self):
        from . import container  # Work around circular deps

        self.docker = AsyncDockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = EventQueue()
        self.config = DaemonConfig()
        self.cmgr = container.ContainerManager(self)

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
