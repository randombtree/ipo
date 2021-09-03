"""
Parse 'pio' command line.

Acts as an entry-point for all subcommands.
"""

import argparse

# TODO: Dynamically probe for cmd_*?
from . import cmd_daemon
from . import cmd_misc
from . import cmd_container

SUBCOMMANDS=[cmd_daemon, cmd_misc, cmd_container]

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
    for m in SUBCOMMANDS:
        m.add_subcommand(subparser)
    args = parser.parse_args()
    # The subparsers add entry points to the namespace in the form of a callable function
    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()
