""" Container task handlers """
import socket
import logging

from .. messagetask import MessageTaskHandler, MessageHandler
from ...api import message


log = logging.getLogger(__name__)


@MessageHandler(message_type = message.FindOrchestrator)
class HandleFindOrchestrator(MessageTaskHandler):
    """ Handle find orchestrator """
    async def handler(self, initial_msg: message.IconMessage):
        ip = initial_msg.ip
        log.debug('Find orchestrator for client %s', ip)
        result = await self.icond.router.find_orchestrators(ip)
        listing = list(map(lambda metric: dict(ip = socket.inet_ntoa(metric.ip),
                                               rtt = metric.rtt), result))
        reply_msg = initial_msg.create_reply(metrics = listing)
        await self._send(reply_msg)
