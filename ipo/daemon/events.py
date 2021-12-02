""" ICOND common events """
# pylint: disable=too-few-public-methods

from ..api.message import IconMessage


class ShutdownEvent:
    """ Event signalling a shutdown """
    ...


class MessageEvent:
    """ Message delivery event """
    msg: IconMessage

    def __init__(self, msg: IconMessage):
        """ msg: The icon message """
        self.msg = msg

    def get(self):
        """ Get the message """
        return self.msg


class ContainerEventBase:
    """ Base event class for container events """
    def __init__(self, container):
        self.container = container


class ContainerRunningEvent(ContainerEventBase):
    """ Event when container is ready to run """
    ...


class ContainerFailedEvent(ContainerEventBase):
    """ Event when a container fails """
    ...
