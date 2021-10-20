"""
ICON sample application

Quick developer mode
python3 -m aiohttp.web -H localhost -P 9998 iconsrv.main:web_init
"""
import sys
import os

from typing import Union

from aiohttp import web

from ipo.client.iconclient import IconClient


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


async def web_init(argv):
    """
    Initializer for webserver.
    """
    global icon_client
    # TODO: Fix hard coded socket value somehow
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
