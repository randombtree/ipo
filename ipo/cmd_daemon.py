"""
Daemon control commands.

Note that we defer loading much of the cruft until an action is decided to speed
up the overall command line experience.
"""
import socket
import argparse
from collections.abc import Iterable
from .api import message
from . import argparsehelper
from .cmdhelper import send_and_receive

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


def bootstrap_daemon(namespace: argparse.Namespace):
    """
    Bootstrap node
    """
    ip = get_ipaddr(namespace.ip)
    port = namespace.port
    reply = send_and_receive(message.BootstrapNode(ip = ip, port = port))
    if reply is not None:
        print(reply)
    else:
        print('Failed!')


def get_ipaddr(host: str) -> str:
    """
    Return the ip address of host (or if it's a proper ip address returns self).

    Exceptions: OSError
    """
    try:
        socket.inet_aton(host)
        return host
    except OSError:
        ...
    # Try to resolve it if its a hostname
    return socket.gethostbyname(host)


class IPChecker(Iterable):
    """ Fake container to check for ip address in parser """
    def __contains__(self, item):
        if not isinstance(item, str):
            return False
        try:
            get_ipaddr(item)
            return True
        except OSError:
            ...
        return False

    def __iter__(self):
        yield 'a.b.c.d'


class PortChecker(Iterable):
    """ Fake container to check for proper port range in parser """
    def __contains__(self, item):
        return 0 < item < 65535

    def __iter__(self):
        # Can't give the whole range as it would be printed
        # on error :(
        yield 1


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

    bootstrap = action.add_parser('bootstrap', help = 'Bootstrap global network view from other node')
    bootstrap.set_defaults(func = bootstrap_daemon)
    # Error messages suck on these, but at least they work as should and ip
    # should be 'acceptable' and port in the correct range
    bootstrap.add_argument('ip', choices = IPChecker(),
                           help = 'Remote IP or hostname')
    bootstrap.add_argument('port', type = int, choices = PortChecker(),  # see comment in PortChecker..
                           help = 'Remote port address 1-65535')
