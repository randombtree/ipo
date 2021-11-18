"""
A Common message task handler meant to handle new incoming messages and help with
multiplexing connection id's.
"""
import asyncio
from asyncio import Queue
from asyncio.streams import StreamReader, StreamWriter
import logging

from abc import ABCMeta, abstractmethod

from typing import (
    Union,
    Type,
    Optional,
    Any,
    AsyncGenerator,
)

from . events import (
    ShutdownEvent,
    MessageEvent,
)
from . state import Icond
from ..api import message
from ..api.message import IconMessage, JSONWriter, MessageReader

from ..util.asynctask import AsyncTask, AsyncTaskRunner

log = logging.getLogger(__name__)


class MessageTaskHandler(metaclass = ABCMeta):
    """ Base class for message handler routines """
    outqueue: Queue
    events: Queue
    task: Union[None, asyncio.tasks.Task]
    icond: Icond

    def __init__(self, outqueue: Queue, icond: Icond, **params):
        """
        outqueue: A queue where this handler can write messages to,
        """
        self.outqueue = outqueue
        self.icond = icond
        self.events = Queue()
        self.task = None
        for k, v in params.items():
            if hasattr(self, k):
                raise RuntimeError(f'Invalid parameter {self} already has {k} defined')
            setattr(self, k, v)

    def post(self, msg: IconMessage):
        """ Post a new message to the handler """
        self.events.put_nowait(MessageEvent(msg))

    def shutdown(self):
        """ Order this handler to quit asap """
        self.events.put_nowait(ShutdownEvent())

    def get_task(self):
        """ Return the task; can be used to wait for the task """
        return self.task

    def run(self, initial_msg: message.IconMessage):
        """
        Start running the handler.
        """
        self.task = asyncio.create_task(self.handler(initial_msg))
        return self.task

    @abstractmethod
    async def handler(self, initial_msg: message.IconMessage):
        # pylint: disable=unused-argument
        """
        Message handles should implement this.
        """
        ...


# Can be used in annotating mappings Message -> Handler
MessageToHandler = dict[Type[message.IconMessage], Type[MessageTaskHandler]]


class MessageFlusher:
    """ Simple flusher of messages from queue to writer """
    writer: JSONWriter
    queue: Queue

    def __init__(self, writer: StreamWriter):
        self.writer = JSONWriter(writer)
        self.queue = Queue()

    async def run(self):
        """ Start flushing """
        while True:
            msg = await self.queue.get()
            if not isinstance(msg, IconMessage):
                log.error('Invalid message queued %s: %s', type(msg), msg)
                continue

            await self.writer.write(msg)
            self.queue.task_done()

    def close(self):
        """ Close writer """
        self.writer.close()


class MessageTaskDispatcher:
    """
    Simple message -> handler mapper.

    Quits on shutdown and also allows the caller introduce own tasks or messages to handle
    itself.
    """
    flusher: MessageFlusher
    reader: MessageReader
    handlers: MessageToHandler
    icond: Icond
    handler_params: dict[str, Any]
    runner: AsyncTaskRunner

    flusher_task: Optional[AsyncTask]
    reader_task: Optional[AsyncTask]
    shutdown_task: Optional[AsyncTask]

    quit_context: Any

    def __init__(self, reader: StreamReader, writer: StreamWriter, handlers: MessageToHandler, icond: Icond, **handler_params):
        self.flusher = MessageFlusher(writer)
        self.reader = MessageReader(reader)
        self.handlers = handlers
        self.icond = icond
        self.handler_params = handler_params

        self.runner = AsyncTaskRunner()

        self.flusher_task = None
        self.reader_task = None
        self.shutdown_task = None

        self.quit_context = None

    async def _process_messages(self) -> AsyncGenerator[Union[AsyncTask, IconMessage], None]:
        """
        Process messages incoming. Generator will yield unhandled tasks and messages.
        Stops on shutdown, exception, or else caller must arrange a task to abort with.
        """
        msg_handlers: dict[str, MessageTaskHandler] = {}
        msg_tasks: set[AsyncTask] = set()
        async for task in self.runner:
            if task == self.reader_task:
                if task.exception():
                    log.debug('Connection closed')
                    self.reader.close()
                    return
                msg = task.result()
                if msg.msg_id in msg_handlers:
                    msg_handlers[msg.msg_id].post(msg)
                else:
                    t = type(msg)
                    if t in self.handlers:
                        handler = self.handlers[t](self.flusher.queue, icond = self.icond, **self.handler_params)
                        msg_tasks.add(self.runner.run(handler.handler(msg),
                                                      restartable = False))
                        msg_handlers[msg.msg_id] = handler
                    else:
                        log.debug('Not handling message %s', msg)
                        yield msg
            elif task in msg_tasks:
                # Task finished
                msg_tasks.remove(task)
            elif task == self.shutdown_task:
                return
            else:
                log.debug('Not handling task %s', task)
                yield task

    def __enter__(self):
        if self.quit_context:
            raise RuntimeError('Cannot re-enter context!')
        self.flusher_task = self.runner.run(self.flusher.run, restartable = False)
        self.reader_task = self.runner.run(self.reader.read)

        self.quit_context = self.icond.subscribe_event(ShutdownEvent)
        quit_queue = self.quit_context.__enter__()
        self.shutdown_task = self.runner.run(quit_queue.get, restartable = False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.runner.clear()
        self.flusher.close()
        self.quit_context.__exit__(exc_type, exc_value, traceback)

    def __aiter__(self):
        if not self.quit_context:
            raise RuntimeError('Use context manager before iterating!')
        return self._process_messages()
