"""
Connections to other orchestrators are handled here.
"""
import asyncio
from asyncio import Queue
import logging
from typing import Optional
from enum import Enum

from . events import ShutdownEvent
from .. util.asynctask import AsyncTaskRunner
from .messagetask import MessageTaskDispatcher, MessageTaskHandler, get_message_handlers
from . import orchestratortask

from ..api import message
from . state import Icond
from .. import __version__ as VERSION
from ..util.signal import Signal, Emitter

log = logging.getLogger(__name__)


class ConnectionException(Exception):
    """ Something went wrong while connecting """
    ...


class OrchestratorConnection:
    """
    A connection to an orchestrator.
    """
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    icond: Icond

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, icond: Icond):
        self.reader = reader
        self.writer = writer
        self.icond = icond

    async def initiate_handshake(self) -> tuple[str, int]:
        """ Initiate handshake (outgoing connection) """
        writer = message.JSONWriter(self.writer)
        reader = message.MessageReader(self.reader)
        await writer.write(message.OrchestratorHello(ip = self.icond.get_ip_address(),
                                                     port = self.icond.config.port,
                                                     version = VERSION))
        msg = await asyncio.wait_for(reader.read(), 60)
        if not isinstance(msg, message.OrchestratorHelloReply):
            raise ConnectionException('Invalid handshake message')
        return (msg.ip, msg.port)

    async def wait_handshake(self) -> tuple[str, int]:
        """ Wait for handshake (incoming connection) """
        reader = message.MessageReader(self.reader)
        writer = message.JSONWriter(self.writer)
        msg = await asyncio.wait_for(reader.read(), 60)
        if not isinstance(msg, message.OrchestratorHello):
            raise ConnectionException('Invalid handshake message')
        await writer.write(msg.create_reply(ip = self.icond.router.get_current_ip(),
                                            port = self.icond.config.port,
                                            version = VERSION))
        return (msg.ip, msg.port)

    async def close(self):
        ...


ConnectionState = Enum('ConnectionState', 'CONNECTING DISCONNECTED CONNECTED FAILED')


class Orchestrator(Emitter):
    """
    Represents a foreign orchestrator.
    """
    HANDLERS = get_message_handlers(orchestratortask)
    Connected = Signal()
    Disconnected = Signal()

    state: ConnectionState
    peer_adr: tuple[str, int]
    icond: Icond
    connection: Optional[OrchestratorConnection]
    inqueue: Queue

    def __init__(self, ip_port: tuple[str, int], icond: Icond):
        self.peer_addr = ip_port
        self.icond = icond
        self.connection = None
        self.state = ConnectionState.DISCONNECTED
        self.inqueue = Queue()

    def is_connected(self) -> bool:
        """ Is the orchestrator connected? """
        return self.state not in [ConnectionState.DISCONNECTED, ConnectionState.FAILED]

    async def new_connection(self, connection: OrchestratorConnection):
        """
        Run with a new connection. Will run until connection fails.
        """
        self.state = ConnectionState.CONNECTED
        await self.Connected()  # type: ignore
        self.connection = connection
        async with MessageTaskDispatcher(connection.reader, connection.writer, self.HANDLERS, self.icond) as dispatcher:
            command_task = dispatcher.run_task(self.inqueue.get)
            async for task, _outqueue in dispatcher:
                if command_task == task:
                    # Currently we are only fed new sessions
                    handler_cls, params, queue = task.result()
                    handler = dispatcher.new_session(handler_cls, msg = None, **params)
                    await queue.put(handler)

        log.debug('Disconnecting from orchestrator %s', self.peer_addr)
        self.state = ConnectionState.DISCONNECTED
        await self.Disconnected()  # type: ignore

    async def start_session(self, handler_cls: type[MessageTaskHandler], **params) -> MessageTaskHandler:
        """ Start a new session to communicate with the remote orchestrator """
        log.debug('Start new session to remote orchestrator of type %s', handler_cls.__name__)
        reply_queue: Queue[MessageTaskHandler] = Queue()
        await self.inqueue.put((handler_cls, params, reply_queue))
        return await reply_queue.get()

    async def connect(self):
        """
        Try to connect to the orchestrator. Will run until connection fails.
        """
        self.state = ConnectionState.CONNECTING
        ip, port = self.peer_addr
        try:
            reader, writer = await asyncio.open_connection(host = ip, port = port)
            connection = OrchestratorConnection(reader, writer, self.icond)
            await connection.initiate_handshake()
            await self.new_connection(connection)
        except OSError:
            log.debug('Connect failed', exc_info = True)
            self.state = ConnectionState.FAILED
            # TODO: Retry etc.
            await self.Disconnected()  # type: ignore

    async def wait_until_connected(self, timeout = None):
        """ Convinience method for waiting until connected
        throws ConnectionException if it times out or is disconnected.
        """
        if self.is_connected():
            return
        async with self.Connected as connected, self.Disconnected as disconnected:
            done, pending = await asyncio.wait([connected, disconnected],
                                               timeout = timeout,
                                               return_when = 'FIRST_COMPLETED')
            for t in pending:
                t.cancel()
            if len(done) < 1:
                raise ConnectionException('Timed out')
            # Perhaps under severe load there can could many signals queued..?
            *_, last = done
            event = last.result()
            log.debug('Event: %s', event)
            if event.signal == self.Disconnected:
                raise ConnectionException('Disconnected')
            # Ok, good to go for now
            return


class OrchestratorManager:
    """ Manage connections to other orchestrators """
    icond: Icond
    orchestrators: dict[tuple[str, int], Orchestrator]
    runner: AsyncTaskRunner

    def __init__(self, icond: Icond):
        self.icond = icond
        self.orchestrators = {}
        self.runner = AsyncTaskRunner()

    async def _run_server(self):
        """ Connection listener for server inbound connections """
        port = self.icond.config.port
        # Note: uses self as callback for new connections (se __call__ below)
        server = await asyncio.start_server(self, port = port)
        async with server:
            await server.serve_forever()

    async def _shutdown_waiter(self):
        with self.icond.subscribe_event(ShutdownEvent) as shutdown_event:
            await shutdown_event.get()

    async def run(self):
        """ Run orchestrator manager """
        self.runner.run(self._run_server())
        shutdown_task = self.runner.run(self._shutdown_waiter())

        async for task in self.runner:
            try:
                # Just provoke the exception if task failed
                _result = task.result()
                if shutdown_task == task:
                    log.debug('Shutting down..')
                    break
            except Exception:
                # Todo: Ignore connection failed tasks
                log.warning('Exception in handler', exc_info = True)
        self.runner.clear()

    async def get_orchestrator(self, ip_port: tuple[str, int]) -> Orchestrator:
        """
        Get an orchestrator for the supplied peer address
        """
        log.debug('Get orchestrator %s', ip_port)
        if ip_port in self.orchestrators:
            log.debug('Using existing orchestrator')
            orchestrator = self.orchestrators[ip_port]
        else:
            log.debug('New orchestrator')
            orchestrator = Orchestrator(ip_port, self.icond)
            self.orchestrators[ip_port] = orchestrator
        if not orchestrator.is_connected():
            log.info('Connecting to orchestrator at %s', ip_port)
            self.runner.run(orchestrator.connect())
        return orchestrator

    async def _handle_incoming(self, connection):
        try:
            ip_port = await connection.wait_handshake()

            if ip_port in self.orchestrators:
                orchestrator = self.orchestrators[ip_port]
                if orchestrator.is_connected():
                    log.warning('Incoming connection from already connected orchestrator')
                    return
                elif orchestrator.state == ConnectionState.CONNECTING:
                    log.debug('Two way connection race. Aborting')
                    return
            orchestrator = Orchestrator(ip_port, self.icond)
            self.orchestrators[ip_port] = orchestrator
            await orchestrator.new_connection(connection)
        except asyncio.TimeoutError:
            log.warning('Timed out waiting for handshake')
            return
        except ConnectionException:
            log.warning('Discarding incoming connection: Handshake failed')
            return

    async def __call__(self, reader, writer):
        """ Handle incoming connection from asyncio server """
        peer = writer.get_extra_info('peername')
        log.info('Incoming connection from %s', peer)
        connection = OrchestratorConnection(reader, writer, self.icond)
        await self._handle_incoming(connection)
        writer.close()
