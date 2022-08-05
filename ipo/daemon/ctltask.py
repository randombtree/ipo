""" ICONctl task handlers """
import asyncio
import socket
import logging

from . messagetask import MessageTaskHandler, MessageToHandler
from ..api import message
from . container import ContainerState
from . container.image import ImageException


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

        ports = msg.publish   if 'env' in msg else {}
        environment = msg.env if 'publish' in msg else {}

        log.debug('Run local ICON %s', msg.image)
        try:
            coordinator = await self.icond.cmgr.start_local_icon(msg.image,
                                                                 ports,
                                                                 environment)
        except ImageException as e:
            await self._send(message.Error.reply_to(msg, msg = f'Image failed: {e}'))
            return

        container = coordinator.container
        async with container.StateChanged as signal:
            while container.state not in [ContainerState.RUNNING, ContainerState.FAILED]:
                try:
                    # FIXME: Wait for 60 seconds; handy when developing, but might need adjustments
                    event = await asyncio.wait_for(signal, 60)
                    new_state = event['state']
                    log.debug('%s state changed to %s', container, new_state)
                except asyncio.TimeoutError:
                    reply_msg = message.Error.reply_to(msg,
                                                       msg = 'Took too long to start (continue in background')
                    break
        reply_msg = \
            msg.create_reply(msg = 'ICON started successfully') if container.state == ContainerState.RUNNING else \
            message.Error.reply_to(msg, msg = 'Failed to start container')
        await self._send(reply_msg)
        await self.outqueue.join()   # Wait until message is sent
        log.debug('ContainerRun finished')


class ContainerLsTask(MessageTaskHandler):
    """ Container listing task """
    async def handler(self, initial_msg: message.IconMessage):
        log.debug('Container ls')
        containers = self.icond.cmgr.list()
        props = {c.deployment.image.full_name: dict(state =  c.state.name, container =  c.container_name)
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
        listing = list(map(lambda metric: dict(ip=socket.inet_ntoa(metric.ip),
                                               port=metric.port,
                                               rtt=metric.rtt), result))
        reply_msg = initial_msg.create_reply(metrics=listing)
        await self.outqueue.put(reply_msg)


# Message -> Handler
CTL_HANDLERS = {
    message.Shutdown: ShutdownTask,
    message.ContainerRun: ContainerRunTask,
    message.ContainerLs: ContainerLsTask,
    message.BootstrapNode: BootstrapNodeTask,
    message.FindOrchestrator: FindOrchestratorTask,
}  # type: MessageToHandler
