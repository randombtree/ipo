"""
A Common message task handler meant to handle new incoming messages and help with
multiplexing connection id's.
"""
import asyncio
from asyncio import Queue
from abc import ABCMeta, abstractmethod

from typing import Union, Type

from . events import (
    ShutdownEvent,
    MessageEvent,
)
from . state import Icond
from ..api.message import IconMessage
from ..api import message


class MessageTaskHandler(metaclass = ABCMeta):
    """ Base class for message handler routines """
    outqueue: Queue
    events: Queue
    task: Union[None, asyncio.tasks.Task]
    icond: Icond

    def __init__(self, outqueue: Queue, icond: Icond):
        """
        outqueue: A queue where this handler can write messages to,
        """
        self.outqueue = outqueue
        self.icond = icond
        self.events = Queue()
        self.task = None

    def post(self, msg: IconMessage):
        """ Post a new message to the handler """
        self.events.put_nowait(MessageEvent(msg))

    def shutdown(self):
        """ Order this handler to quit asap """
        self.events.put_nowait(ShutdownEvent())

    def get_task(self):
        """ Return the task; can be used to wait for the task """
        return self.task

    def run(self, initial_msg: message.IconMessage):
        """
        Start running the handler.
        """
        self.task = asyncio.create_task(self.handler(initial_msg))
        return self.task

    @abstractmethod
    async def handler(self, initial_msg: message.IconMessage):
        # pylint: disable=unused-argument
        """
        Message handles should implement this.
        """
        ...


# Can be used in annotating mappings Message -> Handler
MessageToHandler = dict[Type[message.IconMessage], Type[MessageTaskHandler]]
