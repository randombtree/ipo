"""
A Common message task handler meant to handle new incoming messages and help with
multiplexing connection id's.
"""
import sys
import asyncio
from asyncio import Queue, Task
from asyncio.streams import StreamReader, StreamWriter
import itertools
import logging

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable

from typing import (
    Union,
    Type,
    TypeVar,
    Optional,
    Any,
    AsyncGenerator,
    Mapping,
)
from types import ModuleType

from . events import (
    ShutdownEvent,
    MessageEvent,
)
from . state import Icond
from ..api import message
from ..api.message import IconMessage, JSONWriter, MessageReader

from ..util.asynctask import AsyncTask, AsyncTaskRunner, waitany, RunnableTask

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

    async def _sendmsg(self, msg_cls: type[IconMessage], **params):
        """
        Quck message sending; requires session_id construction or msg_id parameter
        """
        assert 'msg_id' in params or hasattr(self, 'session_id')
        session_id = params.pop('msg_id') if 'msg_id' in params else getattr(self, 'session_id')
        await self._send(msg_cls(msg_id = session_id, **params))

    async def _send(self, msg: IconMessage):
        await self.outqueue.put(msg)

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

    def __await__(self):
        return self.events.get().__await__()

    def _mark_message_handled(self):
        """
        Update queue consumer stats.
        """
        self.events.task_done()

    async def __aenter__(self):
        """
        Context manager for message handling. Returns the message and
        upon exiting will mark it as handeled.
        """
        return await self

    async def __aexit__(self, exc_type, exc, tb):
        self._mark_message_handled()
        return False

    def __aiter__(self):
        return self._async_iter()

    async def _async_iter(self):
        # ITerator helper
        # Iterate until a shutdown event is received
        while True:
            event = await self
            if isinstance(event, ShutdownEvent):
                # Don't mark message handled, upstream must mark it when
                # it has completed its shutdown..
                return
            yield event
            # Control is back, event has been handled!
            self._mark_message_handled()

    @abstractmethod
    async def handler(self, initial_msg: message.IconMessage):
        # pylint: disable=unused-argument
        """
        Message handles should implement this.
        """
        ...


MessageHandlerMapping = Mapping[Type[message.IconMessage], Type[MessageTaskHandler]]
# Can be used in annotating mappings Message -> Handler
MessageToHandler = dict[Type[message.IconMessage], Type[MessageTaskHandler]]


class MessageHandler:
    """
    Annotation for message handlers to auomatically register the handler to the module set of
    handlers.
    """
    MODULE_VAR = 'MESSAGE_HANDLERS'
    message_type: Type[message.IconMessage]

    def __init__(self, message_type: Type[message.IconMessage]):
        self.message_type = message_type

    def __call__(self, cls: Type[MessageTaskHandler]):
        module = sys.modules[cls.__module__]
        mapping: MessageToHandler
        if not hasattr(module, self.MODULE_VAR):
            mapping = {}
            setattr(module, self.MODULE_VAR, mapping)
        else:
            mapping = getattr(module, self.MODULE_VAR)
        mapping[self.message_type] = cls
        return cls


def get_message_handlers(module: ModuleType) -> MessageHandlerMapping:
    """ Return registered message handlers for module """
    if not hasattr(module, MessageHandler.MODULE_VAR):
        raise NameError('Module has no registered message handlers')
    return getattr(module, MessageHandler.MODULE_VAR)


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

    async def drain(self, aws: Iterable[Task] = iter([])):
        """
        Drain the out-queue.
        aws: Give up drain if one of these exit (ex. timeout and/or flusher task),
             the aws tasks will be canceled when finished!
        """
        # Wait a while for the drain
        _, pending = await waitany(itertools.chain(aws, [asyncio.create_task(self.queue.join())]))
        for task in pending:
            task.cancel()

    def close(self):
        """ Close writer """
        self.writer.close()


class ConnectionClosedException(Exception):
    """ Connection closed """
    ...


MessageHandlerType = TypeVar('MessageHandlerType', bound = MessageTaskHandler)


class MessageTaskDispatcher:
    """
    Simple message -> handler mapper.

    Quits on shutdown and also allows the caller introduce own tasks or messages to handle
    itself.
    """
    flusher: MessageFlusher
    reader: MessageReader
    handlers: MessageToHandler
    icond: Optional[Icond]
    handler_params: dict[str, Any]
    runner: AsyncTaskRunner

    flusher_task: Optional[AsyncTask]
    reader_task: Optional[AsyncTask]
    shutdown_task: Optional[AsyncTask]

    quit_context: Any

    msg_handlers: dict[str, MessageTaskHandler]
    msg_tasks: set[AsyncTask]

    def __init__(self, reader: StreamReader, writer: StreamWriter, handlers: MessageHandlerMapping, icond: Optional[Icond], **handler_params):
        """
        If Icond is provided, will watch for shutdown event and exit gracefully when that happens. Else the
        caller must handle it self (e.g. cancel)
        """
        self.flusher = MessageFlusher(writer)
        self.reader = MessageReader(reader)
        self.handlers = dict(handlers)
        self.icond = icond
        self.handler_params = handler_params

        self.runner = AsyncTaskRunner()

        self.flusher_task = None
        self.reader_task = None
        self.shutdown_task = None

        self.quit_context = None

        self.msg_handlers = {}
        self.msg_tasks = set()

    async def write(self, msg: IconMessage):
        """ Write a message to the output stream """
        await self.flusher.queue.put(msg)

    def add_handlers(self, handlers: MessageHandlerMapping):
        """ Add new message handlers """
        self.handlers.update(handlers)

    def run_task(self, task: RunnableTask, *params, restartable: bool = True) -> AsyncTask:
        """ Run a task """
        return self.runner.run(task, *params, restartable = restartable)

    def add_session(self, sessid: str, handler: MessageTaskHandler):
        """
        Add a new session handler. This won't generally do much as it only can receive messages,
        concider using new_session for a more flexible handler.
        DEPRECATED!
        """
        self.msg_handlers[sessid] = handler

    def new_session(self, handler_cls: type[MessageHandlerType], msg: message.IconMessage = None, **kwargs) -> MessageHandlerType:
        """
        Start a new handler specified by handler_cls, allocating a new ID.
        """
        # TODO: MessageTaskHandler needs a rewamp, but work around with this quirk
        # Handle both incoming and outgoing messages in one ugly swoop
        while msg is None:
            msg = message.IconMessage()
            # Paranoid: This shouldn't happen with UUIDs
            if msg.msg_id in self.msg_handlers:
                msg = None
            else:
                log.debug('New outbound session %s', msg.msg_id)
        handler = handler_cls(self.flusher.queue, icond = self.icond, session_id = msg.msg_id, **kwargs)
        self.msg_handlers[msg.msg_id] = handler
        # A 'new' outbound connection will get the initialization message with the correct ID.
        task = self.runner.run(handler.handler(msg),
                               restartable = False)
        self.msg_tasks.add(task)
        return handler

    async def _process_messages(self) -> AsyncGenerator[tuple[Union[AsyncTask, IconMessage], Queue], None]:
        """
        Process messages incoming. Generator will yield unhandled tasks and messages.
        Stops on shutdown, exception, or else caller must arrange a task to abort with.
        """
        msg_handlers = self.msg_handlers
        msg_tasks = self.msg_tasks
        async for task in self.runner:
            if task == self.reader_task:
                e = task.exception()
                if e:
                    log.debug('Reader closed')
                    raise ConnectionClosedException() from e
                msg = task.result()
                log.debug('Received message %s', msg)
                if msg.msg_id in msg_handlers:
                    msg_handlers[msg.msg_id].post(msg)
                else:
                    t = type(msg)
                    if t in self.handlers:
                        log.debug('Message starts new inbound session')
                        self.new_session(self.handlers[t], msg, **self.handler_params)
                    else:
                        log.debug('Not handling message %s', msg)
                        yield (msg, self.flusher.queue)
            elif task == self.flusher_task:
                # Flusher can currently only exit with exception
                e = task.exception()
                log.debug('Flusher closed')
                raise ConnectionClosedException() from e
            elif task in msg_tasks:
                # Task finished
                msg_tasks.remove(task)
            elif task == self.shutdown_task:
                return
            else:
                log.debug('Not handling task %s', task)
                yield (task, self.flusher.queue)

    async def __aenter__(self):
        if self.flusher_task is not None:
            raise RuntimeError('Cannot re-enter context!')
        self.flusher_task = self.runner.run(self.flusher.run, restartable = False)
        self.reader_task = self.runner.run(self.reader.read)

        if self.icond:
            self.quit_context = self.icond.subscribe_event(ShutdownEvent)
            quit_queue = self.quit_context.__enter__()
            self.shutdown_task = self.runner.run(quit_queue.get, restartable = False)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.reader.close()
        # Make sure handlers listening for messages shut down orderly
        for handler in self.msg_handlers.values():
            handler.shutdown()
        # Some operations can take quite a while to finish in task handlers
        # so we really don't want to start canceling stuff that might lead to
        # really weird mid-states.
        exc_info = (exc_type, exc_value, traceback) if exc_type is not None else None
        log.debug('Waiting for tasks to finish',
                  exc_info=exc_info if exc_type != ConnectionClosedException else None)
        if self.msg_tasks:
            await asyncio.wait(map(lambda t: t.asynctask, self.msg_tasks))
        suppress_exc = False
        if exc_type != ConnectionClosedException:
            # Try drain for 1.5 secs
            await self.flusher.drain([self.flusher_task.asynctask,
                                      asyncio.create_task(asyncio.sleep(1.5))])
        else:
            suppress_exc = True
        # Cancel remainders
        self.runner.clear()
        self.flusher.close()
        if self.icond:
            self.quit_context.__exit__(exc_type, exc_value, traceback)
        # Suppress asyncio debug splat
        # This happens with the shutdown message at least
        # when the shutdown event races with the closing socket
        if self.reader_task.is_running():
            await asyncio.wait([self.reader_task.asynctask])
        self.reader_task.exception()

        log.debug('Finished..')
        return suppress_exc

    def __aiter__(self):
        if self.flusher_task is None:
            raise RuntimeError('Use context manager before iterating!')
        return self._process_messages()
