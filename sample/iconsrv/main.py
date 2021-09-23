"""
ICON sample application

Quick developer mode
python3 -m aiohttp.web -H localhost -P 9998 iconsrv.main:web_init
"""
import sys
import os

from aiohttp import web


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
async def index(request):
    """ Dummy index to test that it works """
    return web.Response(text='Hello world')


async def web_init(argv):
    """
    Initializer for webserver.
    """
    # TODO: ipo initializer
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
