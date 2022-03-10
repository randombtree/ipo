""" Container task handlers """
import socket
import logging

from ... util.signal import Event
from ... util.asynctask import AsyncTaskRunner
from ... misc import default
from ... api import message
from .. events import (
    ShutdownEvent,
    MessageEvent,
)
from .. messagetask import MessageTaskHandler, MessageHandler
from . import container as mod_container

log = logging.getLogger(__name__)


@MessageHandler(message_type = message.FindOrchestrator)
class HandleFindOrchestrator(MessageTaskHandler):
    """ Handle find orchestrator """
    async def handler(self, initial_msg: message.IconMessage):
        ip = initial_msg.ip
        log.debug('Find orchestrator for client %s', ip)
        result = await self.icond.router.find_orchestrators(ip)
        listing = list(map(lambda metric: dict(ip = socket.inet_ntoa(metric.ip),
                                               port = metric.port,
                                               rtt = metric.rtt), result))
        reply_msg = initial_msg.create_reply(metrics = listing)
        await self._send(reply_msg)


@MessageHandler(message_type = message.ClientMigrate)
class HandleClientMigrate(MessageTaskHandler):
    """ Handle client migration request """
    from . import container as mod_container  # pylint: disable=import-outside-toplevel

    async def handler(self, initial_msg: message.IconMessage):
        ip = initial_msg.ip
        port = initial_msg.port
        log.debug('Migrate to %s %d', ip, port)
        # Work around imports
        # TODO: Better architecture
        container: mod_container.Container = getattr(self, 'container')
        deployment = await container.coordinator.migrate_to(ip, port)
        deployment.StateChanged.connect(self.events)

        runner = AsyncTaskRunner()
        event_task = runner.run(self.events.get)
        async for task in runner:
            event = task.result()
            if event_task == task:
                self._mark_message_handled()
                if isinstance(event, ShutdownEvent):
                    log.debug('Shutting down...')
                    break
                if isinstance(event, MessageEvent):
                    # Atm. not accepting new msgs, but might change

                    msg = event.msg
                    log.warning('Unhandled msg %s', msg)
                elif isinstance(event, Event):
                    ip = default(deployment.remote_ip, "")
                    ports = default(deployment.remote_ports, {})
                    deployment_state = event['state'].name
                    log.debug('Deployment state change %s, ip: %s, ports: %s', deployment_state, ip, ports)
                    await self._sendmsg(message.ClientDeploymentStatus,
                                        state = deployment_state,
                                        ip = ip,
                                        ports = ports)
        log.debug('Stopping')
