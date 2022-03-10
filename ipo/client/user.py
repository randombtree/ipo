"""
The ICON user is the client software running at the end user.
To avoid confusion between the ICON client, which is the ICON software
communicating with the ICON orchestrator we will use the term user to
denote the end user client as the "user".
"""
from asyncio import Queue
import logging

from .. daemon.messagetask import MessageTaskHandler
from .. daemon.events import MessageEvent
from .. api import message
from . import iconclient


log = logging.getLogger(__name__)


class MigrationProxy(MessageTaskHandler):
    """ Simple task handler for receiving one message from orchestrator """
    result: Queue
    ip: str

    def __init__(self, *args, **kwargs):
        self.ip = kwargs.pop('ip')
        MessageTaskHandler.__init__(self, *args, **kwargs)
        self.result = Queue()

    async def handler(self, initial_msg: message.IconMessage):
        await self._sendmsg(message.FindOrchestrator, ip = self.ip)
        async with self as ev:
            if isinstance(ev, MessageEvent):
                msg =  ev.msg
                if message.OrchestratorListing.match(msg):
                    await self.result.put(msg['metrics'])
                else:
                    log.warning('Unhandled msg %s', msg)
            else:
                log.warning('Unhandled %s', ev)

    async def get_result(self) -> list[dict]:
        """ Returns the result of the orchestrator listing query """
        return await self.result.get()


class User:
    """
    End user.
    """
    client: iconclient.IconClient
    ip: str

    def __init__(self, client: iconclient.IconClient, ip: str):
        self.client = client
        self.ip = ip

    async def get_closest_orchestrator(self):
        """ Ask for the closest orchestrator for this user """
        log.debug('Find orchestrator for %s', self.ip)
        migration = await self.client.start_session(MigrationProxy, ip = self.ip)
        metrics = await migration.get_result()
        log.debug('Got metrics %s', metrics)
        return metrics

    async def migrate_to(self, ip: str, port: int):
        """
        Start migration to the orchestrator.
        Returns a success.
        """
        log.debug('Starting migration to %s:%d', ip, port)
        deployment = await self.client.migrate_to(ip, port)
        return deployment
