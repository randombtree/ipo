"""
A Icon message manager.
"""
import asyncio
from asyncio import Queue
import logging
from contextlib import asynccontextmanager

from abc import ABCMeta, abstractmethod

from typing import (
    Union,
    TypeVar,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Mapping,
    Protocol,
)
from dataclasses import dataclass

from ..api import message
from ..api.message import IconMessage

from ..util.asynctask import AsyncTask, AsyncTaskRunner


log = logging.getLogger(__name__)


class StreamEvent:
    """ Base class for stream handler event """
    # pylint: disable=too-few-public-methods
    ...


@dataclass
class StreamMessageEvent(StreamEvent):
    """ Stream event, incoming message """
    msg: IconMessage


class StreamShutdownEvent(StreamEvent):
    """ Stream event, shutdown """
    # pylint: disable=too-few-public-methods
    ...


class AsyncOutputStream(Protocol):
    """ Protocol for sending ICON messages """
    async def send(self, msg: IconMessage):
        """ Send an ICON message """
        ...


class AsyncInputStream(Protocol):
    """ Protocol for receiving ICON messages """
    async def receive(self) -> IconMessage:
        """ Receive an ICON message """
        ...


class MessageStreamHandler(metaclass = ABCMeta):
    """
    Base handler class.
    """
    msg_id: message.IconId
    outqueue: Queue[IconMessage]
    events: Queue[StreamEvent]

    def __init__(self, msg_id: message.IconId, outqueue: Queue[IconMessage], **params):
        super().__init__()
        self.msg_id = msg_id
        self.outqueue = outqueue
        self.events = Queue()
        for k, v in params.items():
            setattr(self, k, v)

    async def shutdown(self):
        """ Order this handler to shut down """
        await self.add_event(StreamShutdownEvent())

    async def add_event(self, event: StreamEvent):
        """ Insert new event to event queue to be handled by the running task """
        await self.events.put(event)

    async def _send(self, msg_cls, **params):
        await self.outqueue.put(msg_cls(msg_id = self.msg_id, **params))

    @abstractmethod
    async def run(self):
        """ Insert stream handler code here, will be run in own task. """
        ...

    def __await__(self):
        return self.events.get().__await__()

    def __aiter__(self):
        return self._async_iter()

    async def _async_iter(self):
        # ITerator helper
        # Iterate until a shutdown event is received
        while True:
            event = await self
            if isinstance(event, StreamShutdownEvent):
                # Don't mark message handled, upstream must mark it when
                # it has completed its shutdown..
                return
            yield event
            # Control is back, event has been handled!
            self.events.task_done()


HandlerType = TypeVar('HandlerType', bound = MessageStreamHandler)


class MessageStreamManager:
    """
    Manages message stream (de-)muxing.
    """
    reader: AsyncInputStream
    writer: AsyncOutputStream
    runner: AsyncTaskRunner
    handler_params: Mapping[str, Any]    # Parameters when creating stream handler

    outqueue: Queue[IconMessage]

    registered_handlers: dict[type[IconMessage], type[MessageStreamHandler]]

    msg_handlers: dict[message.IconId, MessageStreamHandler]
    msg_tasks: dict[AsyncTask, message.IconId]

    def __init__(self, reader: AsyncInputStream, writer: AsyncOutputStream, runner: AsyncTaskRunner, **params):
        """
        reader: Icon message reader (input)
        writer: Icon message writer (output)
        runner: Task runner
        params: Parameters supplied to every launched handler.
        """
        self.reader = reader
        self.writer = writer
        self.runner = runner

        self.outqueue = Queue()

        self.handler_params = params

        self.registered_handlers = {}

        self.msg_handlers = {}
        self.msg_tasks = {}

    def new_session(self, handler_cls: type[HandlerType], **params) -> HandlerType:
        """
        Start new message session.
        handler_cls: The handler class to launch
        params: Additional parameters to handler (in addition to those supplied in the constructor/cmgr)
        """
        for msg_id in IconMessage.id_generator():
            if msg_id not in self.msg_handlers:
                return self._run_handler(handler_cls, msg_id, **params)
        raise Exception('Bug in generator')  # Silence warnings

    def add_handlers(self, handlers: Mapping[type[IconMessage], type[MessageStreamHandler]]):
        """ Add message handlers for incoming new messages """
        self.registered_handlers.update(handlers)

    def _run_handler(self, handler_cls: type[HandlerType], msg_id: message.IconId, **params) -> HandlerType:
        handler_params = {**self.handler_params, **params}
        handler = handler_cls(msg_id, self.outqueue, **handler_params)
        task = self.runner.run(handler.run())
        self.msg_handlers[msg_id] = handler
        self.msg_tasks[task] = msg_id
        log.debug('New session %s, handler %s', msg_id, handler)
        return handler

    async def _flusher(self):
        """ Outqueue flusher """
        # Allows a unified error-free interface to stream handlers;
        # Exception here is handled in the manager
        while True:
            msg = await self.outqueue.get()
            await self.writer.send(msg)
            self.outqueue.task_done()

    async def _process_messages(self) -> AsyncGenerator[Union[AsyncTask, IconMessage], None]:
        """ Process incoming messages, yield unhandled messages & tasks """
        flusher_task = self.runner.run(self._flusher())
        reader_task = self.runner.run(self.reader.receive)

        async for task in self.runner:
            if task == flusher_task:
                # It's an exception
                e = task.exception()
                log.debug('Flusher exited %s', e)
                break
            elif task == reader_task:
                e = task.exception()
                if e:
                    log.debug('Reader closed')
                    break
                msg: IconMessage = task.result()
                handler = self.msg_handlers.get(msg.msg_id)
                if handler is None:
                    t = type(msg)
                    if t in self.registered_handlers:
                        handler = self._run_handler(self.registered_handlers[t], msg.msg_id)
                if handler is None:
                    log.debug('Unhandled %s, yield up', msg)
                    yield msg
                else:
                    await handler.add_event(StreamMessageEvent(msg = msg))
            elif task in self.msg_tasks:
                msg_id = self.msg_tasks.pop(task)
                handler = self.msg_handlers.pop(msg_id)
                log.debug('Session %s, handler %s closed', msg_id, handler)
            else:
                log.debug('Unhandled task %s, yield up', task)
                yield task

        log.debug('Message processing ends')
        self.runner.remove_task(flusher_task)

    async def _shutdown(self):
        """ Shut down all handlers """
        log.debug('Manager shutdown')
        for handler in self.msg_handlers.values():
            await handler.shutdown()
        # Give some time for handlers to quit
        wait_tasks = [task.asynctask for task in self.msg_tasks]
        if wait_tasks:
            await asyncio.wait(wait_tasks, timeout = 10)
        self.msg_handlers.clear()
        self.msg_tasks.clear()

    def __aiter__(self):
        """
        Process messages; yield unhandled tasks/messages.
        """
        return self._process_messages()

    @classmethod
    @asynccontextmanager
    async def handle(cls, reader: AsyncInputStream, writer: AsyncOutputStream, close_on_exit = True, **params) -> AsyncIterator['MessageStreamManager']:
        """
        Handle message streams.
        reader: Icon message reader (input)
        writer: Icon message writer (output)
        close_on_exit: Invoke close on reader/writer if they exist on exit
        params: Parameters supplied to every launched handler.
        """
        async with AsyncTaskRunner.create(exit_timeout = 10) as runner:
            manager = cls(reader, writer, runner, **params)

            # Control back to caller:
            yield manager
            await manager._shutdown()

            # Only close if stream has appropriate method
            if close_on_exit:
                for stream in [reader, writer]:
                    if hasattr(stream, 'close'):
                        getattr(stream, 'close')()

            # Task runner will cancel remaining tasks
