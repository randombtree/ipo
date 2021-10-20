"""
Container commands. These mimic the "docker container" class commands.
"""
import argparse
from collections import defaultdict
from . import argparsehelper
from .cmdhelper import send_and_receive
from .api import message
from .connection import Connection


def run_container(namespace: argparse.Namespace):
    """
    Instruct the daemon to construct a new container from an image and start running it.
    """
    print(f'here in run container {namespace}')
    image = namespace.image
    msg = message.ContainerRun(image = image)
    reply = send_and_receive(msg)
    if reply is not None:
        print(reply)
    else:
        print('Failed')

async def ls_container(namespace: argparse.Namespace):
    """
    Ask daemon for container listing
    """
    connection = await Connection.connect()
    await connection.write(message.ContainerLs())
    containers = await connection.read()
    if not isinstance(containers, message.ContainerListing):
        raise Exception('Communication error {containers}')
    listing = [dict(name = 'Container', state = 'State', container = 'Docker container')]
    listing.extend([dict(name = k, state = v['state'], container = v['container'])
                    for k, v in containers.containers.items()])
    # Field widths
    width = defaultdict(int)  # type: dict[str, int]
    for c in listing:
        for k, v in c.items():
            width[k] = max(width[k], len(v) + 1)
    formatstr = '{name:%ds}{state:%ds}{container:%ds}' % (width['name'], width['state'], width['container'])
    for c in listing:
        print(formatstr.format(**c))

def add_subcommand(subparser: argparsehelper.AddParser):
    """
    container subcommands
    """
    parser = subparser.add_parser('container',
                                  description = 'ICON control commands. Create new containers and run them.',
                                  help = 'Commands to control ICONs')
    action = parser.add_subparsers(dest = 'action', required = True)

    # KISS for now
    run = action.add_parser('run', help = 'Run a new ICON')
    run.set_defaults(func = run_container)
    run.add_argument('image')

    # List containers
    ls = action.add_parser('ls', help = 'List ICONs')
    ls.set_defaults(func = ls_container)
