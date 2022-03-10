"""
Orchestrator message task handlers
"""
import logging
from typing import Optional

from .. util.asynctask import AsyncTaskRunner
from .. util.signal import Event
from . events import (
    ShutdownEvent,
)
from .. api import message
from . import state
from . messagetask import MessageTaskHandler, MessageHandler
from . container.image import ImageException
from . container.deployment import DeploymentState
from . container.coordinator import ForeignDeploymentCoordinator


log = logging.getLogger(__name__)


@MessageHandler(message_type = message.MigrationRequest)
class DeploymentConnection(MessageTaskHandler):
    """
    Handle incoming migration request and forward status changes back to 'root'.
    """
    # In these states the container is 'running', i.e. dynamic port mappings etc. are available
    CONTAINER_LAUNCHED_STATE = [DeploymentState.CONWAITING, DeploymentState.RUNNING]

    response_sent: bool
    coordinator: Optional[ForeignDeploymentCoordinator]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response_sent = False

    async def _send_state(self, new_state: DeploymentState):
        """ Send a state update to remote """
        assert self.coordinator is not None
        if not self.response_sent and new_state in self.CONTAINER_LAUNCHED_STATE:
            # TODO: Multi-IP environments might need to get this from container mappings
            # Probably update the port mapping API
            ip = state.Icond.instance().get_ip_address()
            # Container updates the port info once it's up and running
            await self._sendmsg(message.MigrationResponse, ip = ip, ports = self.coordinator.info.ports)
            self.response_sent = True
        await self._sendmsg(
            message.DeploymentStateChanged,
            state = new_state.name)

    async def handler(self, initial_msg: message.IconMessage):
        msg = initial_msg
        image_name = msg['image_name']
        ports = msg['ports']
        environment = msg['environment']

        try:
            self.coordinator = await state.Icond.instance().cmgr.start_foreign_icon(image_name, ports, environment)
        except ImageException as e:
            # FIXME: Log less info when not in debug mode
            log.warning('Image failed to load?', exc_info = True)
            await self._sendmsg(message.Error, msg = str(e))
            return
        assert self.coordinator is not None  # Shut up checkers
        # If container is already up and running, completes the migration:
        await self._send_state(self.coordinator.container.state)
        self.coordinator.container.StateChanged.connect(self.events)
        runner = AsyncTaskRunner()
        events_task = runner.run(self.events.get)
        async for task in runner:
            result = task.result()
            if events_task == task:
                event = result
                if isinstance(event, Event):
                    # In case this is changed in the future - guard here:
                    assert event.is_signal(self.coordinator.container.StateChanged)
                    new_state = event['state']

                    await self._send_state(new_state)
                elif isinstance(event, ShutdownEvent):
                    break
