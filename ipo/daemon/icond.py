"""
ICOND server; starting and stopping.
"""

import sys
import asyncio
import os
import json
import uuid
import argparse
from typing import Union, Any
from collections.abc import Callable

import docker  # type: ignore

from . state import Icond, ShutdownEvent
from . message import (IconMessage, InvalidMessage, JSONReader, JSONWriter)
from . import message
from . eventqueue import GlobalEventQueue, Subscription
from . ctltask import CTL_HANDLERS, MessageTaskHandler

ICOND_REPO = "icond_repository"   # ICON local repository
ICOND_CTL_SOCK = "/var/run/icond/icond.sock"   # ICON control socket


async def waitany(tset: set[asyncio.Task]) -> tuple[asyncio.Task, asyncio.Task]:
    """ Convinience function to wait for any async task completion """
    return await asyncio.wait(tset, return_when = asyncio.FIRST_COMPLETED)


class AsyncTask:
    """
    Wrapper around some async task to be run in the AsyncTaskRunner
    """
    fctry: Callable[[], Any]
    _restartable: bool

    def __init__(self, fctry: Callable[[], Any], restartable: bool = True):
        """
        fctry: The factory method that creates an async task to wait for.
        restartable: If set to false the factory method will run only once.
        """
        self.fctry = fctry
        self._restartable = restartable
        self._asynctask = None

    @property
    def restartable(self):
        """ Get the restartable property """
        return self._restartable

    @property
    def asynctask(self):
        """
        Get the underlying async task.
        """
        return self._asynctask

    def start(self):
        """
        Start the task; Should only be used by the task runner.
        """
        assert self._asynctask is None or self._asynctask.done()
        self._asynctask = self.fctry()
        return self._asynctask

    def result(self):
        """ Get the underlying task result """
        assert self._asynctask.done()
        return self._asynctask.result()

    def exception(self):
        """ Get the underlying exception, if any """
        assert self._asynctask.done()
        return self._asynctask.exception()


class AsyncTaskRunner:
    """
    Manager of a set of tasks that have differing run-lengths.
    """
    active: dict[asyncio.Task, AsyncTask]
    completed: set[asyncio.Task]

    def __init__(self):
        self.active = dict()    # Currently running (or completed) tasks
        self.completed = set()  # Previously completed asynctasks

    def start_task(self, task: AsyncTask):
        """ Add and start the task """
        asynctask = task.start()
        self.active[asynctask] = task

    def remove_task(self, task: AsyncTask):
        """ Remove a task from the runner. If it's still active it will be canceled """
        asynctask = task.asynctask
        if asynctask in self.completed:
            # This is just being overly cautious:
            if asynctask in self.completed:
                self.completed.remove(asynctask)
        # This will hinder it from running next time
        # Also, cancel task or it might linger on for basically forever
        if asynctask in self.active:
            del self.active[asynctask]
            if not (asynctask.done() or asynctask.cancelled()):
                asynctask.cancel()

    async def waitany(self) -> list[AsyncTask]:
        """ Wait for any completed tasks, returns list of AsyncTasks that completed """
        # Re-arm tasks that completed last round
        # We don't re-arm them earlier as to allow
        # smoother removals of tasks
        for oldtask in self.completed:
            if oldtask in self.active:
                task = self.active[oldtask]
                del self.active[oldtask]
                # Re-start task
                if task.restartable:
                    asynctask = task.start()
                    self.active[asynctask] = task
                # one-shot tasks won't get re-added, thus will disappear
        # Wait
        done, _pending = await waitany(set(self.active.keys()))
        self.completed = done
        return set(self.active[t] for t in done)


async def iconctl_connection_handler(reader, writer, icond: Icond):
    """ Icon control channel handler """
    reader = JSONReader(reader)
    writer = JSONWriter(writer)
    msg_handlers = dict()  # type: dict[str, MessageTaskHandler]  # msg_id -> TaskObject
    msg_tasks    = dict()  # type: dict[AsyncTask, str]           # task -> msg_id
    asyncrunner = AsyncTaskRunner()
    outqueue = asyncio.Queue()     # Messages queued for transfer

    async def outqueue_flusher():
        """
        All outbound traffic goes thru the outqueue. In this coroutine
        we flush the queue to the real pipe.
        """
        while True:
            msg = await outqueue.get()
            await writer.write(msg)
            outqueue.task_done()

    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        # The shutdown message helps us to tear down the connection
        # while waiting for peer actions (i.e. reading, writing)
        shutdown_task = AsyncTask(lambda: asyncio.create_task(shutdown_event.get()), restartable = False)
        asyncrunner.start_task(shutdown_task)
        # The reader is always active to receive new messages
        read_task = AsyncTask(lambda: asyncio.create_task(reader.read()))
        asyncrunner.start_task(read_task)

        flush_task = AsyncTask(lambda: asyncio.create_task(outqueue_flusher()), restartable = False)
        asyncrunner.start_task(flush_task)
        while not icond.shutdown:
            completed = await asyncrunner.waitany()
            if shutdown_task in completed:
                assert isinstance(shutdown_task.result(), ShutdownEvent)
                # TODO: Graceful shutdown to client
                break

            # If the queue flusher dies there is no receiver left, just quit
            if flush_task in completed:
                # Yeah, could do some diagnostics what happened.. whatever :)
                e = flush_task.exception()
                print('Client closed the receive end (connection died?) {e}')
                break

            # There was something to read
            if read_task in completed:
                completed.remove(read_task)
                e = read_task.exception()
                if e is not None:
                    # Task threw exception, connection probably died
                    print(f'Connection error {e}')
                    break
                try:
                    msg = read_task.result()
                    print(f'Got line {msg}')
                    msg = IconMessage.from_dict(msg)
                    print(f'Received message: {msg}')
                    if isinstance(msg, message.Shutdown):
                        print('Shutting down')
                        reply_msg = msg.create_reply(msg = 'Shutting down')
                        icond.do_shutdown()
                    elif msg.msg_id in msg_handlers:
                        print('Posting message to existing handler')
                        msg_handlers[msg.msg_id].post(msg)
                    else:
                        # New task 'connection'
                        t = type(msg)
                        if t in CTL_HANDLERS:
                            handler = CTL_HANDLERS[t](outqueue, icond)
                            task = AsyncTask(lambda: handler.run(msg), restartable = False)
                            asyncrunner.start_task(task)
                            msg_tasks[task] = msg.msg_id
                            msg_handlers[msg.msg_id] = handler
                        else:
                            print(f'Not handling message {msg}')

                except (json.JSONDecodeError, InvalidMessage) as e:
                    print(f'Invalid message: {e.msg}')
                    reply_msg = IconMessage(IconMessage.TYPE_ERROR, "connection", msg = e.msg)
                    await outqueue.put(reply_msg)

            # A task handler finished?
            for task in completed:
                if task not in msg_tasks:
                    print(f'Unknown task finished? Unhandled: {task}')
                    continue
                # Just cleanup
                msg_id = msg_tasks[task]
                handler = msg_handlers[msg_id]
                del msg_handlers[msg_id]
                del msg_tasks[task]
                print(f'Handler {handler} finished')
    print('Connection closed')


def iconctl_connection_factory(icond: Icond):
    """ Create a connection handler with icond included in the closure """

    async def connection_handler(reader, writer):
        return await iconctl_connection_handler(reader, writer, icond)
    return connection_handler


class RPCHandler:
    """ RPC command handler base class """
    def __init__(self, stream_id: uuid.UUID):
        self.stream_id = stream_id

    def generate_message(self, msg_type: str, **contents):
        """ Generate a message with current ID attached """
        return IconMessage(msg_type, self.stream_id, **contents)


class OneShotRPCHandler(RPCHandler):
    """ Simple RPC handler that only returns a message """
    def handle(self, msg: IconMessage) -> IconMessage:
        pass


class StreamRPCHandler(RPCHandler):
    """ Persistent RPC handler that will exchange several messages with the same ID """
    RPC_NAME = "undefined"
    task: Union[None, asyncio.tasks.Task]
    in_queue: asyncio.Queue
    out_queue: asyncio.Queue

    def __init__(self, stream_id: uuid.UUID, out_queue: asyncio.Queue):
        super().__init__(stream_id)
        self.out_queue = out_queue
        self.in_queue = asyncio.Queue()
        self.task = None

    def start(self) -> asyncio.tasks.Task:
        """
        Start the RPC handler; this involves running the run method as a task

        Returns the task for the caller to wait on.
        """
        self.task =  asyncio.create_task(self.run(), name = f"rpchandler-{self.RPC_NAME}")
        return self.task

    def put(self, item):
        """
        Put an item to the tasks processing queue - this most probably is a message.
        """
        try:
            self.in_queue.put_nowait(item)
        except asyncio.QueueFull:
            # Task has failed somehow, kill it
            self.task.cancel("Task couldn't keep up with the messages")

    async def run(self):
        """ Async method to run as part of the handler """


async def iconctl_server(icond: Icond):
    """ ICON control channel server """
    os.makedirs(os.path.dirname(ICOND_CTL_SOCK), exist_ok = True)
    try:
        os.unlink(ICOND_CTL_SOCK)
    except OSError:
        if os.path.exists(ICOND_CTL_SOCK):
            raise

    server = await asyncio.start_unix_server(
        iconctl_connection_factory(icond),
        path = ICOND_CTL_SOCK
    )
    async with server:
        await server.serve_forever()


class InitializationException(Exception):
    """ Exception inicating something went wrong during initialization """
    ...


def init_repository(icond: Icond):
    print("Initializing repository..")
    # Repository
    try:
        repo = icond.docker.containers.get(ICOND_REPO)
        if repo.status != "running":
            print(f"ICON repository was not running ({repo.status}), starting it..")
            repo.start()
        else:
            print("ICON repository ok")
    except docker.errors.NotFound:
        # First run; create repository
        # TODO: separate init from normal daemon run
        print("Creating ICON local repository..")
        icond.docker.containers.run(
            "registry:2",
            name=ICOND_REPO,
            detach=True,
            restart_policy={"name": "always"},
            ports={"5000/tcp": 5000}       # TODO: Config
        )
        print("Done..")
    except docker.errors.APIError as e:
        print("Failed to communicate with Docker")
        print(e)
        raise InitializationException('Failed to communicate with Docker') from e


async def main():
    """ Icond daemon """
    icond = Icond()

    init_repository(icond)
    print("Starting server")
    # Start the control channel server
    ctl_server_task = asyncio.create_task(iconctl_server(icond),
                                          name = "ctl_server")

    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        shutdown_task = asyncio.create_task(shutdown_event.get())
        (done, _pending) = await waitany({shutdown_task, ctl_server_task})
        # Any task finishing indicates that we want to exit, either due to some internal
        # error or a shutdown event
        if shutdown_task in done:
            print("Shutdown signaled")


def start(params : argparse.Namespace):
    """ Entry point for module run """
    if not (params.force or os.geteuid() == 0):
        print("Starting of the ICON daemon might fail when not run as root...")
        print("Try --force if you are confident it will work")
        sys.exit(-1)

    asyncio.run(main())
