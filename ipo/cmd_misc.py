"""
Misc commands that might go away as development proceeds.

"""
import asyncio
import argparse
from . import argparsehelper


def init_registry(namespace: argparse.Namespace) -> int:
    """
    Quick'n'dirty registry initialization from command line.
    """
    print('Initializing docker registry...')
    from .daemon import icond

    async def runner():
        state = icond.Icond()
        try:
            await icond.init_repository(state)
        except icond.InitializationException as e:
            print('Failed to init registry..')
            print(e)
            return 1
        return 0
    return asyncio.run(runner())


def client_connect(namespace: argparse.Namespace):
    """
    Testing ICON client connection
    """
    from .client.iconclient import IconClient
    from .daemon.config import DaemonConfig

    config = DaemonConfig()
    name = namespace.name
    socket_path = f'{config.run_directory}/ICON_{name}/icon.sock'

    async def client_loop():
        signals = asyncio.Queue()
        client = IconClient(socket_path)
        client.Connected.connect(signals)
        client.Disconnected.connect(signals)
        await client.connect()
        while True:
            event = await signals.get()
            if event.is_signal(client.Connected):
                print('Client connected')
            elif event.is_signal(client.Disconnected):
                print('Client disconnected')

    asyncio.run(client_loop())


def add_subcommand(subparser: argparsehelper.AddParser):
    """
    Add misc commands to ipo.
    """

    parser = subparser.add_parser('misc', help = 'Misc VOLATILE commands. Don\'t use outside IPO development as they will disappear!')

    action = parser.add_subparsers(dest = 'action', required = True)
    reg = action.add_parser('init_registry', help = 'Init local docker registry')
    reg.set_defaults(func = init_registry)

    connect = action.add_parser('connect', help = '(debug hack) Connect to daemon as icon client')
    connect.set_defaults(func = client_connect)
    connect.add_argument('name')
