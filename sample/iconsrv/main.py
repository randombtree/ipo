"""
ICON sample application

Quick developer mode
python3 -m aiohttp.web -H localhost -P 9998 iconsrv.main:web_init
"""
import sys
import os
import json
import logging
import asyncio

from typing import Union

import aiohttp
from aiohttp import web

from ipo.client.iconclient import IconClient
from ipo.util.asynctask import AsyncTaskRunner
from ipo.api.message import (
    IconMessage,
    InvalidMessage,
)
from . message import (
    UserHello,
    UserHelloReply,
    MessageSocket,
)


log = logging.getLogger(__name__)


def env_or(env, default, target = str):
    """ Return environment variable if available, else default value """
    val = os.environ[env] if env in os.environ else default
    try:
        return target(val)
    except ValueError:
        return target(default)


ICON_SOCKET = 'ICON_SOCKET'
CONFIG_PORT = env_or('PORT', 8080, int)

routes = web.RouteTableDef()

icon_client = None  # type: Union[None, IconClient]


@routes.get('/')
async def index(request):
    """ Dummy index to test that it works """
    return web.Response(text=f'Client state: {icon_client.state.name}')


@routes.get('/ws')
async def websocket_handler(request):
    """ Websocket handler """
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
    user = await icon_client.new_user(request.remote)
    await ms.send(UserHelloReply(session_id = 'todo'))
    runner = AsyncTaskRunner()
    ws_read_task = runner.run(ms.receive)
    probe_task = runner.run(user.get_closest_orchestrator())
    async for task in runner.wait_next():
        if task == ws_read_task:
            exc = task.exception()
            if exc:
                log.error('%s: Exception %s receiving messages: %s',
                          request.remote,
                          exc.__class__.__name__,
                          exc)
                break
            msg = task.result()
            log.debug('%s: msg received - %s', request.remote, msg)
        elif task == probe_task:
            result = probe_task.result()
            log.debug('%s', result)
    runner.clear()
    log.debug('%s: WS closed', request.remote)
    return ws


async def web_init(argv):
    """
    Initializer for webserver.
    """
    global icon_client
    client_params = dict()
    if ICON_SOCKET in os.environ:
        client_params['sockname'] = os.environ[ICON_SOCKET]
    icon_client = IconClient(**client_params)
    log.info('Waiting for connection to ICON server')
    connect_waitqueue  = asyncio.Queue()
    icon_client.Connected.connect(connect_waitqueue)
    await icon_client.connect()
    _event = await connect_waitqueue.get()
    connect_waitqueue.task_done()
    log.info('Connected to ICON')
    app = web.Application()
    app.add_routes(routes)
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
