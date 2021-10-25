"""
ICON sample application

Quick developer mode
python3 -m aiohttp.web -H localhost -P 9998 iconsrv.main:web_init
"""
import sys
import os
import json

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
    Hello,
    HelloReply,
    MessageSocket,
)


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
    print('WS connected')
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ms = MessageSocket(ws)

    # Handshake
    msg = await ms.receive()  # type: IconMessage
    if not isinstance(msg, Hello):
        print('Invalid hello')
        return ws
    await ms.send(HelloReply(session_id = 'todo'))
    runner = AsyncTaskRunner()
    ws_read_task = runner.run(ms.receive)
    async for task in runner.wait_next():
        if task == ws_read_task:
            print('msg received')
            exc = task.exception()
            if exc:
                print(f'Exception {exc.__class__.__name__} receiving messages: {exc}')
                break
            msg = task.result()
            print(msg)
    runner.clear()
    print('WS closed')
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
    
    await icon_client.connect()
    app = web.Application()
    app.add_routes(routes)
    return app


def main():
    """
    Main entry point.
    Starts aiohttp web server framework.
    """
    web.run_app(web_init(sys.argv), port=CONFIG_PORT)


if __name__ == '__main__':
    main()
