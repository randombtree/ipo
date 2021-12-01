"""
The ICON user is the client software running at the end user.
To avoid confusion between the ICON client, which is the ICON software
communicating with the ICON orchestrator we will use the term user to
denote the end user client as the "user".
"""
import logging

from ..daemon.messagetask import MessageTaskHandler
from ..daemon.events import MessageEvent
from ..api import message
from . import iconclient

log = logging.getLogger(__name__)


class OrchestratorTask(MessageTaskHandler):
    async def handler(self, initial_msg: message.IconMessage):
        ev = await self
        if isinstance(ev, MessageEvent):
            return ev.msg
        return None


class User:
    """
    End user.
    """
    client: 'iconclient.IconClient'
    ip: str

    def __init__(self, client: 'iconclient.IconClient', ip: str):
        self.client = client
        self.ip = ip

    async def get_closest_orchestrator(self):
        """ Ask for the closest orchestrator for this user """
        log.debug('Find orchestrator for %s', self.ip)
        task = OrchestratorTask(None, None)
        await self.client.start_session(message.FindOrchestrator(ip = self.ip), task)
        msg = await task.handler(None)
        log.debug('%s', msg)
        return msg

    async def migrate_to(self, orchestrator_ip) -> bool:
        """
        Start migration to the orchestrator.
        Returns a success.
        """
        ...
