""" ICONctl task handlers """
from asyncio import Queue
import socket
import logging

from . events import (
    ContainerRunningEvent,
    ContainerFailedEvent
)

from . messagetask import MessageTaskHandler, MessageToHandler
from ..api import message
from . container import ContainerState
from .container.deployment import DeploymentInfo


log = logging.getLogger(__name__)


class ShutdownTask(MessageTaskHandler):
    """ Command daemon shutdown """
    async def handler(self, initial_msg: message.IconMessage):
        self.icond.do_shutdown()
        await self.outqueue.put(initial_msg.create_reply(msg = 'Shutting down'))


class ContainerRunTask(MessageTaskHandler):
    """ Run (start) container from image """
    async def handler(self, initial_msg: message.IconMessage):
        msg = initial_msg
        image = msg.image

        # Set deployment info; from here we only deploy root containers
        deployment = DeploymentInfo(ports = msg.publish if 'env' in msg else {},
                                    environment = msg.env if 'publish' in msg else {},
                                    root = True)

        log.debug('Run container %s, info: %s', msg.image, deployment)

        reply_msg = msg.create_reply(msg = 'Working..')
        wakeup = Queue()  # type: Queue
        self.icond.eventqueue.listen([
            ContainerRunningEvent,
            ContainerFailedEvent,
        ], wakeup)
        container = await self.icond.cmgr.run_container(image, deployment)
        while container.state != ContainerState.RUNNING:
            ev = await wakeup.get()
            wakeup.task_done()
            if isinstance(ev, ContainerRunningEvent) and ev.container == container:
                log.debug('Container %s successfully started', image)
                break
            if isinstance(ev, ContainerFailedEvent) and ev.container == container:
                log.warning('Failed to start container')
                # TODO: Perhaps more data from event
                reply_msg = message.Error(msg_id = msg.msg_id, msg = 'Failed to start container')
                break

        await self.outqueue.put(reply_msg)
        await self.outqueue.join()   # Wait until message is sent
        log.debug('ContainerRun finished')


class ContainerLsTask(MessageTaskHandler):
    """ Container listing task """
    async def handler(self, initial_msg: message.IconMessage):
        log.debug('Container ls')
        containers = self.icond.cmgr.list()
        props = {c.image: dict(state =  c.state.name, container =  c.container_name)
                 for c in containers}
        reply = initial_msg.create_reply(containers = props)
        await self.outqueue.put(reply)
        await self.outqueue.join()


class BootstrapNodeTask(MessageTaskHandler):
    """ Bootstrap DHT node from ip """
    async def handler(self, initial_msg: message.IconMessage):
        assert isinstance(initial_msg, message.BootstrapNode)
        ip = initial_msg.ip
        port = int(initial_msg.port)
        log.debug('Bootstrap node from %s:%d', ip, port)
        success = await self.icond.router.add_node(ip, port)
        reply_msg = initial_msg.create_reply(msg = 'Ok') if success else \
            initial_msg.create_reply(reply_cls = message.Error, msg = 'Failed to contact bootstrap node')
        await self.outqueue.put(reply_msg)


class FindOrchestratorTask(MessageTaskHandler):
    """ Find orchestrator for IP """
    async def handler(self, initial_msg: message.IconMessage):
        assert isinstance(initial_msg, message.FindOrchestrator)
        ip = initial_msg.ip
        result = await self.icond.router.find_orchestrators(ip)
        listing = list(map(lambda metric: dict(ip = socket.inet_ntoa(metric.ip),
                                               port = metric.port,
                                               rtt = metric.rtt), result))
        reply_msg = initial_msg.create_reply(metrics = listing)
        await self.outqueue.put(reply_msg)


# Message -> Handler
CTL_HANDLERS = {
    message.Shutdown: ShutdownTask,
    message.ContainerRun: ContainerRunTask,
    message.ContainerLs: ContainerLsTask,
    message.BootstrapNode: BootstrapNodeTask,
    message.FindOrchestrator: FindOrchestratorTask,
}  # type: MessageToHandler
