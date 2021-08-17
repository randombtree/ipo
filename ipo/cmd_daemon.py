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
    print("Stop daemon")


def add_subcommand(parser: argparsehelper.AddParser):
    """
    Add subcommand details to a subparser.

    NB.: No typing here as the subparser isn't oficially tied to a specific class and might be
    subject to changes, although the current implementation uses _SubParserAction.
    """
    parser = parser.add_parser('daemon', help = 'Control the ICON daemon')

    parser = parser.add_subparsers(dest = 'action', required = True)
    start = parser.add_parser('start', help = 'Start the ICON daemon')
    start.set_defaults(func = start_daemon)   # 'Hack' to give a callpoint for main parser
    start.add_argument('--force', default = False, action = 'store_true')   # Force starting, omitting checks..

    stop = parser.add_parser('stop', help = 'Stop the ICON daemon')
    stop.set_defaults(func = stop_daemon)
