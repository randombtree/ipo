""" ICON client """
from enum import Enum, auto
import asyncio
from asyncio import Task, Queue
import json

from typing import Union

from ..util.asynctask import AsyncTaskRunner
from ..util.signal import Signal, Emitter
from ..daemon import message
from ..daemon.message import MessageReader, JSONWriter


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

    def __init__(self, sockname):
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
                    print('Connection to server failed')
                    connect_task = self.task_runner.run(asyncio.sleep(5))
                    continue
                r = task.result()
                if r is None:
                    print('Timeout; Try to connect to server')
                    # This is after a delay.. try to connect
                    connect_task = self.task_runner.run(
                        asyncio.open_unix_connection(path = self.sockname),
                    )
                else:
                    (reader, writer) = r
                    reader = MessageReader(reader)
                    writer = JSONWriter(writer)
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
                            self.state = ClientState.CONNECTED
                            await self.Connected()
                            continue
                    except json.JSONDecodeError:
                        ...
                    print('Connection handshake failed')
                    connect_task = self.task_runner.run(asyncio.sleep(10))
            elif task == read_task:
                # Is there something to read
                e = task.exception()
                if e:
                    # Read failed; just throw away this connection and try again
                    self.state = ClientState.CONNECTING
                    await self.Disconnected()
                    self.task_runner.remove_task(read_task)
                    # Start reconnecting ASAP
                    connect_task = self.task_runner.run(asyncio.sleep(0))
                    continue
                r = task.result()
                # TODO..
                await self.Connected()
            elif task == inqueue_task:
                r = task.result()
                self.inqueue.task_done()
                if isinstance(r, ShutdownEvent):
                    print('Client will disconnect')
                    break
            else:
                print('BUGBUG! Unhandled task {task}')
        self.state = ClientState.DISCONNECTED
        await self.Disconnected()
        self.task_runner.clear()

