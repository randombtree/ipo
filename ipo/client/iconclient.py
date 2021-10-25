""" ICON client """
from enum import Enum, auto
import asyncio
from asyncio import Task, Queue
import json
import logging

from typing import Union

from ..util.asynctask import AsyncTaskRunner
from ..util.signal import Signal, Emitter
from ..api import message
from ..api.message import MessageReader, JSONWriter


log = logging.getLogger(__name__)


class ShutdownEvent:
    ...


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
    _task: Union[Task, None]
    state: ClientState
    task_runner: AsyncTaskRunner

    def __init__(self, sockname = '/run/icond/icon.sock'):
        """
        Initialize the ICON client.
        sockname: Socket to connect to
        """
        self.sockname = sockname
        self._task = None
        self.state = ClientState.DISCONNECTED
        self.task_runner = AsyncTaskRunner()
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
            # _nowait to guarantee no shutdown race
            self.inqueue.put_nowait(ShutdownEvent())
            await asyncio.wait((self._task,))

    async def _run(self):
        inqueue_task = self.task_runner.run(self.inqueue.get)
        connect_task = self.task_runner.run(asyncio.sleep(0))
        timeout_task = None
        read_task    = None
        writer       = None
        async for task in self.task_runner.wait_next():
            if task == connect_task:
                # Connection task is either a timeout return or a result from the connection attempt
                e = task.exception()
                if e:
                    # Connection failed
                    # Try again in 5 secs..
                    log.warning('Connection to server failed')
                    connect_task = self.task_runner.run(asyncio.sleep(5))
                    continue
                r = task.result()
                if r is None:
                    log.warning('Timeout; Try to connect to server')
                    # This is after a delay.. try to connect
                    connect_task = self.task_runner.run(
                        asyncio.open_unix_connection(path = self.sockname),
                    )
                else:
                    (reader, writer) = r
                    reader = MessageReader(reader)
                    writer = JSONWriter(writer)
                    log.debug('Sending handshake')
                    await writer.write(message.ClientHello(version = '0.0.1'))
                    try:
                        # Wait for the appropriate reply
                        # TODO: Move to appropriate place as this by-passes the task runner
                        reply = await reader.read()
                        if isinstance(reply, message.HelloReply):
                            # TODO: Validation..
                            read_task = self.task_runner.run(
                                reader.read
                            )
                            log.debug('Connected')
                            self.state = ClientState.CONNECTED
                            await self.Connected()
                            continue
                    except (json.JSONDecodeError, message.InvalidMessage) as e:
                        log.error('Error decoding message %s: %s', e.__class__.__name__, e)
                        ...
                    reader.close()
                    writer.close()
                    log.error('Connection handshake failed')
                    connect_task = self.task_runner.run(asyncio.sleep(10))
            elif task == read_task:
                # Is there something to read
                exception = task.exception()
                if exception:
                    # Read failed; just throw away this connection and try again
                    self.state = ClientState.CONNECTING
                    await self.Disconnected()
                    self.task_runner.remove_task(read_task)
                    # Start reconnecting ASAP
                    connect_task = self.task_runner.run(asyncio.sleep(0))
                    continue
                msg = task.result()
                log.debug('ICON server sent us %s', msg)
                # TODO..
            elif task == inqueue_task:
                r = task.result()
                self.inqueue.task_done()
                if isinstance(r, ShutdownEvent):
                    log.debug('Client will disconnect')
                    break
            else:
                log.error('BUGBUG! Unhandled task {task}')
        self.state = ClientState.DISCONNECTED
        await self.Disconnected()
        self.task_runner.clear()

