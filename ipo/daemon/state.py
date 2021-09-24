"""
ICOND global state
"""
from docker import DockerClient

from . eventqueue import GlobalEventQueue, Subscription

class ShutdownEvent:
    ...

class Icond:
    """ Icond global state """
    docker: DockerClient
    shutdown: bool
    eventqueue: GlobalEventQueue

    def __init__(self):
        self.docker = DockerClient(base_url='unix://var/run/docker.sock')
        self.shutdown = False
        self.eventqueue = GlobalEventQueue()

    def do_shutdown(self):
        """ Shutdown daemon commanded """
        self.shutdown = True
        self.eventqueue.publish(ShutdownEvent())

    def subscribe_event(self, event: type) -> Subscription:
        """ Subscribe to icond events """
        return self.eventqueue.subscribe(event)
