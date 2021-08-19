"""
Misc commands that might go away as development proceeds.

"""
import sys
import argparse
from . import argparsehelper


def init_registry(namespace: argparse.Namespace) -> int:
    """
    Quick'n'dirty registry initialization from command line.
    """
    print('Initializing docker registry...')
    from .daemon import icond
    state = icond.Icond()
    try:
        icond.init_repository(state)
    except icond.InitializationException as e:
        print('Failed to init registry..')
        print(e)
        return 1
    return 0


def add_subcommand(parser: argparsehelper.AddParser):
    """
    Add misc commands to ipo.
    """

    parser = parser.add_parser('misc', help = 'Misc VOLATILE commands. Don\'t use outside IPO development as they will disappear!')

    parser = parser.add_subparsers(dest = 'action', required = True)
    reg = parser.add_parser('init_registry', help = 'Init local docker registry')
    reg.set_defaults(func = init_registry)
