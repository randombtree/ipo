"""
Daemon control commands.

Note that we defer loading much of the cruft until an action is decided to speed
up the overall command line experience.
"""
import argparse
from . import argparsehelper


def start_daemon(namespace: argparse.Namespace):
    """
    Handle starting of daemon. Callback from cmdline.
    """
    print("Starting daemon...")
    from .daemon.icond import start
    start(namespace)


def stop_daemon(namespace: argparse.Namespace):
    """
    Handle stopping of daemon. Callback from cmdline
    """

    from .connection import Connection
    from .daemon import message
    import asyncio

    async def send_shutdown():
        conn = await Connection.connect()
        await conn.write(message.Shutdown())
        msg = await conn.read()
        print(msg.as_dict())

    # TODO: This is fairly generic code, split it out
    print('Stop daemon')
    try:
        asyncio.run(send_shutdown())
    except PermissionError:
        print('Permission denied when communicating with daemon')
    except OSError as e:
        print(f'Error {e} when communicating with daemon')


def add_subcommand(subparser: argparsehelper.AddParser):
    """
    Add subcommand details to a subparser.

    NB.: No typing here as the subparser isn't oficially tied to a specific class and might be
    subject to changes, although the current implementation uses _SubParserAction.
    """
    parser = subparser.add_parser('daemon', help = 'Control the ICON daemon')

    action = parser.add_subparsers(dest = 'action', required = True)
    start = action.add_parser('start', help = 'Start the ICON daemon')
    start.set_defaults(func = start_daemon)   # 'Hack' to give a callpoint for main parser
    start.add_argument('--force', default = False, action = 'store_true')   # Force starting, omitting checks..

    stop = action.add_parser('stop', help = 'Stop the ICON daemon')
    stop.set_defaults(func = stop_daemon)
