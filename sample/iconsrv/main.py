"""
ICON sample application

Quick developer mode
python3 -m aiohttp.web -H localhost -P 9998 iconsrv.main:web_init
"""
import sys
import os
import logging

from aiohttp import web

from ipo.client.user import User
from ipo.client.deploymentproxy import DeploymentProxy, DeploymentState
from ipo.util.signal import Event
from ipo.util.messagestream import (
    MessageStreamManager,
    MessageStreamHandler,
    StreamMessageEvent,
)
from ipo.api.message import IconMessage

from . message import (
    UserHello,
    UserHelloReply,
    UserMigrate,
    UserMigrateReply,
    MessageSocket,
)
from . server import ServerState


log = logging.getLogger(__name__)


def env_or(env, default, target = str):
    """ Return environment variable if available, else default value """
    val = os.environ[env] if env in os.environ else default
    try:
        return target(val)
    except ValueError:
        return target(default)


CONFIG_PORT = env_or('PORT', 8080, int)

routes = web.RouteTableDef()


@routes.get('/')
async def index(_request):
    """ Dummy index to test that it works """
    return web.Response(text='Client state:')


def get_remote_port_number(ports: dict[str, int]):
    """
    Retrieve the remote orchestrator port number from supplied
    docker-style port-mapping
    """
    for local_port, external_port in ports.items():
        _port, method = local_port.split('/')  # e.g. '8080/tcp'
        # Currently not using the 'port' to differentiate
        # different service ports as we only use one
        # and 'phony' servers are easyer to set up
        if method == 'tcp':
            return external_port
    raise Exception('Invalid port mapping received')


class ClientMigrationHandler(MessageStreamHandler):
    """ Migration task, only communicates if migration can happen """
    user: User
    remote: str

    async def _try_migrate(self):
        metrics = await self.user.get_closest_orchestrator()
        best = metrics[0]
        our  = metrics[-1]
        best_RTT = best['rtt']
        our_RTT = our['rtt']
        # NOTE: We use no logic currently, migrate blindly if the RTT
        #       is better.
        if not best_RTT < our_RTT:
            log.debug('%s: Migration not worth it', self.remote)
            return

        ip = best['ip']
        port = best['port']
        log.debug('Start migration to %s:%d', ip, port)
        migrate_proxy: DeploymentProxy = \
            await self.user.migrate_to(ip = ip, port = port)
        migrate_proxy.StateChanged.connect(self.events)
        await migrate_proxy.emit_state()  # Slight race/migrate proxy could already be connected
        # Wait for deployment state change (or client disconnect)
        # Allow re-use of the iterator:
        msg_iter = self._async_iter()
        async for event in msg_iter:
            log.debug(event)
            if isinstance(event, Event) and event.is_signal(migrate_proxy.StateChanged):
                state = event['state']
                if state == DeploymentState.RUNNING:
                    port = get_remote_port_number(event['ports'])
                    ip = event['ip']
                    log.debug('Can start migration to %s:%d', ip, port)
                    await self._send(UserMigrate, ip = ip, port = port)
                    break

        # On shutdown, this iterator does nothing:
        async for event in msg_iter:
            if isinstance(event, StreamMessageEvent):
                msg = event.msg
                if UserMigrateReply.match(msg):
                    log.debug('Migrate complete..')
                    return

    async def run(self):
        await self._try_migrate()


@routes.get('/ws')
async def websocket_handler(request):
    """ Websocket handler """
    state = ServerState.instance()
    log.debug('WS connected from %s', request.remote)
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ms = MessageSocket(ws)

    # Handshake
    msg = await ms.receive()  # type: IconMessage
    if not isinstance(msg, UserHello):
        log.warning('Invalid hello from %s', request.remote)
        return ws
    # Register this "user"
    user = await state.client.new_user(request.remote)
    await ms.send(UserHelloReply(session_id = 'todo'))
    async with MessageStreamManager.handle(ms, ms, user = user, remote = request.remote) as manager:
        manager.new_session(ClientMigrationHandler)
        async for unhandled in manager:
            log.debug('Got unhandled %s', unhandled)
    log.debug('%s: WS closed', request.remote)
    return ws


async def web_init(_argv):
    """
    Initializer for webserver.
    """
    app = web.Application()
    app.add_routes(routes)
    # Wait for connection here..
    _state = await ServerState.init()
    return app


def main():
    """
    Main entry point.
    Starts aiohttp web server framework.
    """
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)
    web.run_app(web_init(sys.argv), port=CONFIG_PORT)


if __name__ == '__main__':
    main()
