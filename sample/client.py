#!/usr/bin/env python3
"""
ICON sample client application
"""
import sys
import asyncio
from asyncio import Queue, Task
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
import time
import collections
from typing import Optional, AsyncIterator, Tuple, Any

import aiohttp

from iconsrv.message import (
    UserHello,
    UserHelloReply,
    UserMigrate,
    MessageSocket,
)

from ipo.api.message import IconMessage
from ipo.util.asynctask import AsyncTaskRunner, AsyncTask
from ipo.util.signal import Signal, Emitter, Event


log = logging.getLogger(__name__)


class ClientStats:
    """ Collector of client stats """
    _instance: Optional['ClientStats'] = None
    timer_offset: int
    events: dict[str, list[int]]

    def __init__(self):
        ClientStats._instance = self
        self.timer_offset = 0
        self.events = collections.defaultdict(list)

    def _record(self, event: str):
        now = time.monotonic_ns()
        if self.timer_offset == 0:
            self.timer_offset = now
        # First event starts at 0
        self.events[event].append(now - self.timer_offset)

    @classmethod
    def record(cls, event: str):
        """ Record timestamp for event """
        cls.instance()._record(event)

    @classmethod
    def init(cls):
        """ Initialize this singleton """
        _i = cls()

    @classmethod
    def instance(cls) -> 'ClientStats':
        """ Get singleton instance """
        assert cls._instance is not None
        return cls._instance

    def _dump(self) -> str:
        return '\n'.join([f'{k}: {v}' for k, v in self.events.items()])

    @classmethod
    def dump(cls) -> str:
        """ Dump stats to string format """
        return cls.instance()._dump()


class ClientConnection:
    """ IPO style message connection over web socket """
    connection: aiohttp.ClientWebSocketResponse
    _ms: MessageSocket

    def __init__(self, connection: aiohttp.ClientWebSocketResponse):
        self.connection = connection
        self._ms = MessageSocket(connection)

    async def receive(self) -> IconMessage:
        """ Receive message on this connection """
        msg = await self._ms.receive()
        log.debug('Received %s', msg)
        return msg

    async def send(self, msg: IconMessage):
        """ Send message on this connection """
        await self._ms.send(msg)

    @classmethod
    @asynccontextmanager
    async def connect(cls, host: str, session: aiohttp.ClientSession) -> AsyncIterator['ClientConnection']:
        """
        host  hostname:port
        """
        url = f'http://{host}/ws'
        log.debug('Connecting to %s', url)
        ClientStats.record('ws_connect')
        async with session.ws_connect(url, timeout = 5) as ws:
            log.debug('Connected (ws) to %s', url)
            ClientStats.record('ws_connected')
            yield ClientConnection(ws)
        log.debug('Disconnected from %s', url)


class SrvEvent:
    """ Srv communication event """
    ...


class SrvShutdownEvent(SrvEvent):
    """ Srv shutdown event """
    ...


class SrvSession(Emitter):
    """
    Sample client session to ICON srv
    """
    Connected = Signal()  # Connected to server, also triggered when changing servers

    initial_host: str
    send_queue: Queue[IconMessage]
    recv_queue: Queue[IconMessage]
    cmd_queue: Queue
    session: aiohttp.ClientSession

    @dataclass
    class SrvRunner:
        """ Encapsulate the run state """
        parent: 'SrvSession'
        connection_stack: AsyncExitStack
        connecting_stack: AsyncExitStack

        runner: AsyncTaskRunner

        cmd_task: AsyncTask = field(init = False)
        send_task: Optional[AsyncTask] = None
        # There can be several receiving connections due to deferred disconnects:
        recv_tasks: list[AsyncTask] = field(default_factory = list)
        timeout_tasks: set[AsyncTask] = field(default_factory = set)  # Disconnect timeout(s)
        connect_task: Optional[AsyncTask] = None

        connection_active: Optional[ClientConnection] = None

        def __post_init__(self):
            # One time inits
            self.cmd_task = self.runner.run(self.parent.cmd_queue.get)

        @classmethod
        async def create(cls, parent: 'SrvSession', stack: AsyncExitStack) -> 'SrvSession.SrvRunner':
            """ Async initialization of SrvRunner """
            # Have to play a bit games with the contexts, and as stacks only support
            # adding or clering (pop_all), have to have stacks to move items between
            connection_stack = await stack.enter_async_context(AsyncExitStack())
            connecting_stack = await stack.enter_async_context(AsyncExitStack())

            runner = await stack.enter_async_context(AsyncTaskRunner.create(exit_timeout = 10))
            return cls(parent,
                       connection_stack,
                       connecting_stack,
                       runner)

        async def deferred_disconnect(self, recv_task: AsyncTask, stack: AsyncExitStack):
            """ Disconnect connection after a delay """
            try:
                # Allow cancel to skip waiting
                await asyncio.sleep(2)
            finally:
                # We really, really want cleanup to run
                await stack.aclose()  # This will close down connection
                self.runner.remove_task(recv_task)
                self.recv_tasks.remove(recv_task)

        async def try_connect(self, host):
            """ Try to connect to a host """
            # We do a bit of magic, but since async tasks aren't really multithreaded
            # playing around with the external stack "should be ok"
            log.debug('Trying to connect to %s', host)
            ClientStats.record('connecting')
            async with AsyncExitStack() as stack:
                connection = await stack.enter_async_context(
                    ClientConnection.connect(host, self.parent.session))
                ClientStats.record('UserHello_sent')
                await connection.send(UserHello())
                # Don't wait forever for a reply..
                log.debug('Wait for hello..')

                def log_error():
                    log.error('Timeout waiting for hello')

                stack.callback(log_error)

                msg = await asyncio.wait_for(connection.receive(), timeout = 5)
                if not isinstance(msg, UserHelloReply):
                    log.error('Handshake error, invalid message %s', msg)
                    raise Exception('Failed connecting')
                ClientStats.record('UserHelloReply_received')
                # FUTURE: Logged in user handling
                new_stack = stack.pop_all()
                # Now it's ready to be moved to the connecting stack
                await self.connecting_stack.enter_async_context(new_stack)
                return connection

        async def run(self):
            """ The actual run loop """
            # Start connecting to initial host
            self.connect_task = self.runner.run(
                self.try_connect(self.parent.initial_host))

            async for task in self.runner:
                # NB: For the moment, we'll fail on any unexpected event,
                #     such as failed connection attempts
                result = task.result()
                if self.cmd_task == task:
                    event: SrvEvent = result
                    if isinstance(event, SrvShutdownEvent):
                        return
                    log.warning('Unhandled event %s', event)
                elif self.connect_task == task:
                    connect_task = None
                    connection: ClientConnection = result
                    ClientStats.record('connected')
                    if self.connection_active is not None:
                        # Don't want to miss in flight data, close connection after a
                        # 'reasonable' timeout.
                        # FUTURE: Graceful close without depending on any timeouts?
                        self.runner.remove_task(self.send_task)
                        disconnecting_stack = self.connection_stack.pop_all()
                        # Will ensure that old connection is cleaned up after a while:
                        self.runner.run(self.deferred_disconnect(
                            self.recv_tasks[-1],        # Last is always the 'active'
                            disconnecting_stack))
                        log.debug('Switching active connection ...')
                        ClientStats.record('switch')
                    else:
                        log.debug('Connected..')
                    self.connection_active = connection
                    # Move context to connection_stack
                    new_stack = self.connecting_stack.pop_all()
                    await self.connection_stack.enter_async_context(new_stack)

                    async def send_op(connection):
                        while True:
                            await connection.send(await self.parent.send_queue.get())

                    self.send_task = self.runner.run(send_op(connection))
                    self.recv_tasks.append(self.runner.run(connection.receive))   # Repeating
                    # Aaand we are open for business
                    await self.parent.Connected()
                elif task in self.timeout_tasks:
                    # The actual work is done in deferred_disconnect
                    self.timeout_tasks.remove(task)
                elif task in self.recv_tasks:
                    # Incoming message
                    msg: IconMessage = result
                    if UserMigrate.match(msg):
                        ip = msg['ip']
                        port = msg['port']
                        log.debug('Migration to %s:%d', ip, port)
                        if connect_task is not None:
                            log.warning('Migration message while migration in progress?')
                            continue
                        ClientStats.record('migrate_received')
                        # Start connecting to other server
                        self.connect_task = self.runner.run(
                            self.try_connect(f'{ip}:{port}'))
                    else:
                        await self.parent.recv_queue.put(msg)

                else:
                    log.warning('Unhandled task %s', task)

    def __init__(self, initial_host: str):
        self.initial_host = initial_host
        self.send_queue = Queue()
        self.recv_queue = Queue()
        self.cmd_queue = Queue()
        self.session = aiohttp.ClientSession()

    async def send(self, msg: IconMessage):
        """ Send message to active ICON """
        self.send_queue.put(msg)

    async def receive(self) -> IconMessage:
        """ Receive message from active ICON """
        return await self.recv_queue.get()

    async def _run(self):
        log.debug('Starting srv')
        async with AsyncExitStack() as stack:
            runner = await SrvSession.SrvRunner.create(self, stack)

            def log_end():
                log.debug('Stopping srv')

            stack.callback(log_end)
            await runner.run()

    @classmethod
    @asynccontextmanager
    async def start(cls, initial_host: str) -> AsyncIterator[Tuple['SrvSession', Task[Any]]]:
        """ Start a session to ICON srv """
        srv = SrvSession(initial_host)
        async with AsyncExitStack() as stack:
            task = asyncio.create_task(srv._run())

            def cleanup():
                log.error('Srv session didn\'t exit?')
                task.cancel()
            stack.callback(cleanup)
            yield (srv, task)
            log.debug('Ending session')
            await srv.cmd_queue.put(SrvShutdownEvent())
            await asyncio.wait_for(task, timeout = 10)
            # Remove cleanup
            stack.pop_all()


async def communicate(host) -> int:
    """ Communicate with srv ICON """
    ret: int = 0
    async with AsyncExitStack() as stack:
        ClientStats.record('start')
        runner = await stack.enter_async_context(AsyncTaskRunner.create(exit_timeout = 10))
        (srv, srv_task) = await stack.enter_async_context(SrvSession.start(host))

        print(srv_task, type(srv_task))
        srv_task = runner.add_task(srv_task)
        event_queue: Queue[Event] = Queue()
        srv.Connected.connect(event_queue)
        read_task = runner.run(srv.receive)
        event_task = runner.run(event_queue.get)
        timeout_task = runner.run(asyncio.sleep(120))
        quit_task = None
        connected_events = 0

        async for task in runner:
            result = task.result()
            if read_task == task:
                log.debug('Received message %s', result)
            elif event_task == task:
                event: Event = result
                if event.is_signal(srv.Connected):
                    log.info('Connected')
                    connected_events += 1
                    ClientStats.record('upper_connected')
                    # Quit after migrate
                    # TODO: Specify different exit parameters
                    if connected_events == 2:
                        quit_task = runner.run(asyncio.sleep(0))
                else:
                    log.error('Unhandled event %s', event)
            elif srv_task == task:
                log.debug('srv task quit unexpectedly, exit')
                break
            elif quit_task == task:
                log.debug('Done, quitting')
                break
            elif timeout_task == task:
                log.error('Timeout connecting?')
                ret = 1
                break
    return ret


async def main(argv) -> int:
    """
    Client stub.

    Connect to server websocket and do handshake.
    """
    host = argv[1]
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(pathname)s:%(lineno)3d - %(name)s->%(funcName)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)

    ClientStats.init()

    ret = await communicate(host)
    if not ret:
        print(ClientStats.dump())
    return ret


if __name__ == '__main__':
    sys.exit(asyncio.run(main(sys.argv)))
