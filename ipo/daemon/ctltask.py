""" ICONctl task handlers """
import asyncio
from asyncio import Queue

from typing import Union

import docker

from . events import ShutdownEvent, MessageEvent
from . state import Icond
from . message import IconMessage
from . import message


class MessageTaskHandler:
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

    def run(self, initial_msg):
        """
        Start running the handler.
        """
        self.task = asyncio.create_task(self.handler(initial_msg))
        return self.task

    async def handler(self, initial_msg):
        # pylint: disable=unused-argument
        """
        Message handles should implement this.
        """
        ...


class ContainerRunTask(MessageTaskHandler):
    """ Run (start) container from image """
    async def handler(self, initial_msg):
        msg = initial_msg
        image = msg.image
        print(f'Run container {msg.image}')
        # TODO!:
        # Docker commands are synchronous, so some
        # threading will be needed here (e.g. loop.run_in_executor()); doing some bad blocking
        try:
            docker_image = self.icond.docker.images.get(image)
            print(docker_image)
            reply_msg = msg.create_reply(msg = 'Working..')
        except docker.errors.ImageNotFound:
            reply_msg = IconMessage(IconMessage.TYPE_ERROR, msg_id = msg.msg_id, msg = 'Image not found')

        await self.outqueue.put(reply_msg)
        await self.outqueue.join()   # Wait until message is sent
        print('ContainerRun finished')


# Message -> Handler
CTL_HANDLERS = {
    message.ContainerRun: ContainerRunTask,
}
