"""
Server helpers.
"""
import os
import logging
from asyncio import Queue
from dataclasses import dataclass

from typing import Optional

from ipo.client.iconclient import IconClient
from ipo.util.signal import Event


log = logging.getLogger(__name__)

ICON_SOCKET = 'ICON_SOCKET'


def env_or(env, default, target = str):
    """ Return environment variable if available, else default value """
    val = os.environ[env] if env in os.environ else default
    try:
        return target(val)
    except ValueError:
        return target(default)


@dataclass
class ServerState:
    """
    Server globals.
    """
    client: IconClient
    port: int

    _instance: Optional['ServerState'] = None  # Singleton instance

    @staticmethod
    async def _init_icon() -> IconClient:
        client_params = {}
        if ICON_SOCKET in os.environ:
            client_params['sockname'] = os.environ[ICON_SOCKET]
        icon_client = IconClient(**client_params)
        log.info('Waiting for connection to ICON server')
        connect_waitqueue: Queue[Event]  = Queue()
        icon_client.Connected.connect(connect_waitqueue)
        await icon_client.connect()
        _event = await connect_waitqueue.get()
        assert isinstance(_event, Event)
        connect_waitqueue.task_done()
        log.info('Connected to ICON')
        return icon_client

    @staticmethod
    async def _init_port() -> int:
        return env_or('PORT', 8080, int)

    @classmethod
    async def init(cls) -> 'ServerState':
        """ Initialize server """
        if hasattr(cls, '_instance') and cls._instance is not None:
            return cls._instance

        instance = ServerState(
            client = await cls._init_icon(),
            port = await cls._init_port(),
        )
        cls._instance = instance
        return instance

    @classmethod
    def instance(cls) -> 'ServerState':
        """ Get singleton """
        assert cls._instance is not None
        return cls._instance
