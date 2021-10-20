"""
Parse 'pio' command line.

Acts as an entry-point for all subcommands.
"""
import sys
import argparse
import asyncio
import pkgutil
import importlib
from inspect import iscoroutinefunction


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

    # Dynamically load cmd_* modules
    package = sys.modules[__name__].__package__
    pmod = sys.modules[package]
    for m in pkgutil.iter_modules(path = pmod.__path__):
        if m.ispkg is False and m.name.startswith('cmd_'):
            module = importlib.import_module(f'.{m.name}', 'ipo')
            if not hasattr(module, 'add_subcommand'):
                print(f'WARNING: {m.name} missing add_subcommand and will not work!')
            else:
                module.add_subcommand(subparser)

    args = parser.parse_args()
    # The subparsers add entry points to the namespace in the form of a callable function
    if 'func' in args:
        func = args.func
        if iscoroutinefunction(func):
            asyncio.run(func(args))
        else:
            func(args)
    else:
        parser.print_help()
