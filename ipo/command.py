"""
Parse 'pio' command line.

Acts as an entry-point for all subcommands.
"""

import argparse

from . import cmd_daemon
from . import cmd_misc

def main():
    """
    Main method for the "pio" command line tool.
    """

    # FIXME: Determine the correct progname, especially when invoked with 'python3 -m'
    parser = argparse.ArgumentParser(
        prog = 'ipo',
        description = 'Python ICON orchestrator command line interface'
    )
    subparser = parser.add_subparsers(dest = 'command', help = 'sub-command help')
    cmd_daemon.add_subcommand(subparser)
    cmd_misc.add_subcommand(subparser)
    args = parser.parse_args()
    print(args)
    # The subparsers add entry points to the namespace in the form of a callable function
    args.func(args)
