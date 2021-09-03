"""
Container commands. These mimic the "docker container" class commands.
"""
import argparse
from . import argparsehelper
from .cmdhelper import send_and_receive
from .daemon import message

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

def add_subcommand(parser: argparsehelper.AddParser):
    """
    container subcommands
    """
    parser = parser.add_parser('container',
                               description = 'ICON control commands. Create new containers and run them.',
                               help = 'Commands to control ICONs')
    parser = parser.add_subparsers(dest = 'action', required = True)

    # KISS for now
    run = parser.add_parser('run', help = 'Run a new ICON')
    run.set_defaults(func = run_container)
    run.add_argument('image')
