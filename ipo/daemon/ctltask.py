""" ICONctl task handlers """
from asyncio import Queue
import logging

import docker  # type: ignore

from . events import (
    ContainerRunningEvent,
    ContainerFailedEvent
)

from . messagetask import MessageTaskHandler, MessageToHandler
from ..api import message
from . container import ContainerState


log = logging.getLogger(__name__)


class ContainerRunTask(MessageTaskHandler):
    """ Run (start) container from image """
    async def handler(self, initial_msg: message.IconMessage):
        msg = initial_msg
        image = msg.image
        log.debug('Run container %s', msg.image)
        try:
            docker_image = await self.icond.docker.images.get(image)
            log.debug(docker_image)
            reply_msg = msg.create_reply(msg = 'Working..')
            wakeup = Queue()  # type: Queue
            self.icond.eventqueue.listen([
                ContainerRunningEvent,
                ContainerFailedEvent,
            ], wakeup)
            container = await self.icond.cmgr.run_container(image)
            while container.state != ContainerState.RUNNING:
                ev = await wakeup.get()
                wakeup.task_done()
                if isinstance(ev, ContainerRunningEvent) and ev.container == container:
                    log.debug('Container %s successfully started', image)
                    break
        except docker.errors.ImageNotFound:
            reply_msg = message.Error(msg_id = msg.msg_id, msg = 'Image not found')

        await self.outqueue.put(reply_msg)
        await self.outqueue.join()   # Wait until message is sent
        log.debug('ContainerRun finished')


class ContainerLsTask(MessageTaskHandler):
    """ Container listing task """
    async def handler(self, initial_msg: message.IconMessage):
        log.debug('Container ls')
        containers = self.icond.cmgr.list()
        props = { c.name: dict(state =  c.state.name, container =  c.container_name)
                  for c in containers }
        reply = initial_msg.create_reply(containers = props)
        await self.outqueue.put(reply)
        await self.outqueue.join()


# Message -> Handler
CTL_HANDLERS = {
    message.ContainerRun: ContainerRunTask,
    message.ContainerLs: ContainerLsTask,
}  # type: MessageToHandler
