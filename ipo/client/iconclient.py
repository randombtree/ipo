""" ICON client """
from enum import Enum, auto
import asyncio
from asyncio import Task, Queue
import logging

from typing import Optional

from ..util.signal import Signal, Emitter
from ..api import message
from ..daemon.messagetask import MessageTaskDispatcher

log = logging.getLogger(__name__)


class ClientState(Enum):
    """ Icon client state """
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    SHUTDOWN = auto()


class IconClient(Emitter):
    """ Client connection to ICON orchestrator """
    Connected    = Signal()
    Disconnected = Signal()

    sockname: str
    inqueue: Queue
    _task: Optional[Task]
    state: ClientState

    def __init__(self, sockname = '/run/icond/icon.sock'):
        """
        Initialize the ICON client.
        sockname: Socket to connect to
        """
        self.sockname = sockname
        self._task = None
        self.state = ClientState.DISCONNECTED
        self.inqueue = Queue()

    async def connect(self):
        """
        Start connecting to orchestrator.

        Once a connection is established a connect event will be sent.
        NB: This means on return the client won't necessary be connected.
        """
        # This is mostly teorethical but best to wait out a competing shutdown first
        if self.state == ClientState.SHUTDOWN:
            await asyncio.wait((self._task,))
        if not self.state == ClientState.DISCONNECTED:
            return
        self.state = ClientState.CONNECTING
        self._task = asyncio.create_task(self._run())

    async def disconnect(self):
        """ Disconnect from orchestrator """
        if self.state in (ClientState.CONNECTING, ClientState.CONNECTED):
            self.state = ClientState.SHUTDOWN
            await self._task.cancel()

    async def _communicate(self, reader, writer):
        # We abuse the MessageTaskDispatcher because it takes care of most of
        # the communication woes
        async with MessageTaskDispatcher(reader, writer, {}, None) as dispatcher:
            await dispatcher.write(message.ClientHello(version = '0.0.1'))
            async for unhandled, _out in dispatcher:
                if not self.state == ClientState.CONNECTED:
                    if isinstance(unhandled, message.HelloReply):
                        log.debug('Handshake with orchestrator completed version=%s',
                                  unhandled.version)
                        # TODO: Version etc.
                        self.state = ClientState.CONNECTED
                        await self.Connected()
                        continue
                log.error('Unhandled %s', unhandled)

        if self.state == ClientState.CONNECTED:
            log.debug('Disconnected..')
            self.state = ClientState.DISCONNECTED
            await self.Disconnected()

    async def _run(self):
        while not self.state == ClientState.SHUTDOWN:
            try:
                log.debug('Connecting to orchestrator..')
                self.state = ClientState.CONNECTING
                (reader, writer) = await asyncio.open_unix_connection(path = self.sockname)
                log.debug('Connection established to orchestrator')
            except OSError:
                log.warning('Connection to orchestrator failed')
                await asyncio.sleep(5)
                continue
            await self._communicate(reader, writer)
            await asyncio.sleep(5)
