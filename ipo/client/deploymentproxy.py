"""

"""
import logging

from typing import Optional

from .. daemon.messagetask import MessageTaskHandler
from .. daemon.events import MessageEvent
from .. daemon.container.deployment import DeploymentState
from .. api import message
from .. util.signal import Emitter, Signal


log = logging.getLogger(__name__)


class DeploymentProxy(Emitter, MessageTaskHandler):
    """
    The deployment proxy mirrors the state from the remote deployment here.
    TODO: Two way communication.
    """
    StateChanged = Signal()

    state: DeploymentState
    ip: str
    port: int
    deployment_ip: Optional[str]
    deployment_ports: Optional[list[int]]

    def __init__(self, *args, **kwargs):
        self.ip = kwargs.pop('ip')
        self.port = kwargs.pop('port')
        self.state = DeploymentState.STOPPED
        self.deployment_ip = None
        self.deployment_ports = None
        Emitter.__init__(self)
        MessageTaskHandler.__init__(self, *args, **kwargs)

    async def emit_state(self):
        """ Force to emit the current state """
        await self.StateChanged(state = self.state,
                                ip = self.deployment_ip,
                                ports = self.deployment_ports)

    async def handler(self, initial_msg: message.IconMessage):
        await self._sendmsg(message.ClientMigrate, ip = self.ip, port = self.port)
        async for event in self:
            assert isinstance(event, MessageEvent)
            msg = event.msg
            if message.ClientDeploymentStatus.match(msg):
                new_state = msg['state']
                log.debug('State changed to %s', new_state)
                deployment_ip = msg['ip']
                deployment_ports = msg['ports']
                self.deployment_ip = deployment_ip if len(deployment_ip) > 0 else None
                self.deployment_ports = deployment_ports if len(deployment_ports) > 0 else None
                if new_state in DeploymentState.__members__:
                    new_state = DeploymentState.__members__[new_state]
                    self.state = new_state
                    await self.emit_state()
                else:
                    log.warning('Invalid state %s from orchestrator', new_state)
            else:
                log.warning('Unhandled message %s', msg)

        self._mark_message_handled()


__all__ = [
    'DeploymentProxy',
    'DeploymentState',    # Re-export
]
