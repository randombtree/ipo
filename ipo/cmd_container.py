"""
Container commands. These mimic the "docker container" class commands.
"""
import argparse
from argparse import ArgumentTypeError
from collections import defaultdict
from . import argparsehelper
from .cmdhelper import send_and_receive
from .api import message
from .connection import Connection


def run_container(namespace: argparse.Namespace):
    """
    Instruct the daemon to construct a new container from an image and start running it.
    """
    image = namespace.image
    params = {}
    for k, var in vars(namespace).items():
        if k in {'publish', 'env'}:
            params[k] = dict(var)

    msg = message.ContainerRun(image = image, **params)
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
    listing.append({k: ''.rjust(len(v), '-') for k, v in listing[0].items() })
    # Underlines
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


GENERIC_PORTMAP_ERROR = 'Format not supported, specify host:container port'


def make_portmapping(arg: str) -> tuple[str, str]:
    """
    Map argument port:port to docker format
    Currently mapping to IP isn't supported, and perhaps even uneccessary in ICON.
    (container port/proto, host port)
    """
    split = arg.split(':')
    if len(split) != 2:
        raise ArgumentTypeError(GENERIC_PORTMAP_ERROR)
    host, container = split
    # support only TCP for now
    if not host.isdecimal() or not container.isdecimal():
        raise ArgumentTypeError(GENERIC_PORTMAP_ERROR)
    return (f'{container}/tcp', host)


def make_environment(arg: str) -> tuple[str, str]:
    """
    Parse var=val
    """
    split = arg.split('=')
    if len(split) != 2:
        raise ArgumentTypeError('Specify environment variables as VAR=VAR')
    var, val = list(map(str.strip, split))
    if not var[0].isalpha():
        raise ArgumentTypeError('Invalid environment variable; must start with [a-z]')
    return (var, val)


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
    # Try to mimic docker container
    run.add_argument('--publish', '-p', nargs = '+', action = 'extend',
                     type = make_portmapping,
                     help = 'Publish hostport:containerport')
    run.add_argument('--env', '-e', nargs = '+', action = 'extend',
                     type = make_environment,
                     help = 'Set environment variables in container VAR=VAL')
    run.add_argument('image')

    # List containers
    ls = action.add_parser('ls', help = 'List ICONs')
    ls.set_defaults(func = ls_container)
