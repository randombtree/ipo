"""
ICOND server; starting and stopping.
"""

import sys
import asyncio
import os
import json
import argparse
import logging

import docker  # type: ignore

from . events import ShutdownEvent
from . state import Icond
from ..api.message import (IconMessage, InvalidMessage, JSONReader, JSONWriter)
from ..api import message
from . ctltask import CTL_HANDLERS, MessageTaskHandler
from ..util.asynctask import AsyncTask, AsyncTaskRunner, waitany
from . signals import set_signal_handlers


log = logging.getLogger(__name__)


async def iconctl_connection_handler(reader, writer, icond: Icond):
    """ Icon control channel handler """
    reader = JSONReader(reader)
    writer = JSONWriter(writer)
    msg_handlers = dict()  # type: dict[str, MessageTaskHandler]  # msg_id -> TaskObject
    msg_tasks    = dict()  # type: dict[AsyncTask, str]           # task -> msg_id
    asyncrunner = AsyncTaskRunner()
    outqueue = asyncio.Queue()  # type: asyncio.Queue   # Messages queued for transfer

    async def outqueue_flusher():
        """
        All outbound traffic goes thru the outqueue. In this coroutine
        we flush the queue to the real pipe.
        """
        while True:
            msg = await outqueue.get()
            if isinstance(msg, ShutdownEvent):
                return
            await writer.write(msg)
            outqueue.task_done()

    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        # The shutdown message helps us to tear down the connection
        # while waiting for peer actions (i.e. reading, writing)
        shutdown_task = AsyncTask(shutdown_event.get, restartable = False)
        asyncrunner.start_task(shutdown_task)
        # The reader is always active to receive new messages
        read_task = AsyncTask(reader.read)
        asyncrunner.start_task(read_task)

        flush_task = AsyncTask(outqueue_flusher, restartable = False)
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
                log.debug('Client closed the receive end (connection died?) {e}')
                break

            # There was something to read
            if read_task in completed:
                completed.remove(read_task)
                e = read_task.exception()
                if e is not None:
                    # Task threw exception, connection probably died
                    log.debug('Connection error %s: %s', e.__class__.__name__, e)
                    break
                try:
                    msg = read_task.result()
                    log.debug('Got line %s', msg)
                    msg = IconMessage.from_dict(msg)
                    log.debug('Received message: %s', msg)
                    if isinstance(msg, message.Shutdown):
                        log.info('Shutting down')
                        reply_msg = msg.create_reply(msg = 'Shutting down')
                        await outqueue.put(reply_msg)
                        await outqueue.join()  # Flush outqueue
                        icond.do_shutdown()
                    elif msg.msg_id in msg_handlers:
                        log.debug('Posting message to existing handler')
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
                            log.warning('Not handling message %s',  msg)

                except (json.JSONDecodeError, InvalidMessage) as e:
                    log.warning('Invalid message: %s', e)
                    reply_msg = message.Error(msg = str(e))
                    await outqueue.put(reply_msg)

            # A task handler finished?
            for task in completed:
                if task not in msg_tasks:
                    log.error('Unknown task finished? Unhandled: %s', task)
                    continue
                # Just cleanup
                msg_id = msg_tasks[task]
                handler = msg_handlers[msg_id]
                del msg_handlers[msg_id]
                del msg_tasks[task]
                log.debug('Handler %s finished', handler)
        # Make sure to output writer quits
        await outqueue.put(ShutdownEvent())
        await flush_task.asynctask
    log.debug('Connection closed')


def iconctl_connection_factory(icond: Icond):
    """ Create a connection handler with icond included in the closure """

    async def connection_handler(reader, writer):
        return await iconctl_connection_handler(reader, writer, icond)
    return connection_handler


async def iconctl_server(icond: Icond):
    """ ICON control channel server """
    control_socket = icond.config.control_socket
    try:
        os.unlink(control_socket)
    except OSError:
        if os.path.exists(control_socket):
            raise

    server = await asyncio.start_unix_server(
        iconctl_connection_factory(icond),
        path = control_socket
    )
    async with server:
        await server.serve_forever()


class InitializationException(Exception):
    """ Exception inicating something went wrong during initialization """
    ...


async def init_repository(icond: Icond):
    """ Initialize the docker repository (registry) """
    log.info('Initializing repository..')
    # Repository
    repository = icond.config.repository
    try:
        repo = await icond.docker.containers.get(repository)
        if repo.status != "running":
            log.info('ICON repository was not running (%s), starting it..', repo.status)
            await repo.start()
        else:
            log.debug('ICON repository ok')
    except docker.errors.NotFound:
        # First run; create repository
        # TODO: separate init from normal daemon run
        log.info('Creating ICON local repository..')
        await icond.docker.containers.run(
            "registry:2",
            name=repository,
            detach=True,
            restart_policy={"name": "always"},
            ports={"5000/tcp": 5000}       # TODO: Config
        )
        log.info('Done..')
    except docker.errors.APIError as e:
        print("Failed to communicate with Docker")
        print(e)
        raise InitializationException('Failed to communicate with Docker') from e


async def main():
    """ Icond daemon """
    icond = Icond()
    os.makedirs(os.path.dirname(icond.config.run_directory), exist_ok = True)

    await init_repository(icond)
    log.info('Starting server')

    ctl_server_task = asyncio.create_task(iconctl_server(icond),
                                          name = "ctl_server")
    cmgr_task = icond.cmgr.start()

    set_signal_handlers(icond)
    with icond.subscribe_event(ShutdownEvent) as shutdown_event:
        log.info('Server started')
        shutdown_task = asyncio.create_task(shutdown_event.get())
        (done, _pending) = await waitany({
            shutdown_task,
            ctl_server_task,
            cmgr_task,
        })
        for task in done:
            e = task.exception()
            if e:
                log.error('There was an exception in the daemon: %s', e, exc_info = True)
                task.print_stack()
        # Any task finishing indicates that we want to exit, either due to some internal
        # error or a shutdown event
        if shutdown_task in done:
            log.info('Shutdown signaled')

        # Graceful shutdown for cmgr
        if cmgr_task not in done:
            log.info('Waiting for tasks to shut down..')
            # FIXME: This could take a lot of time, some way to ensure that progress is made
            #        should probalby be added instead of using timeouts
            await asyncio.wait({cmgr_task}, timeout = 60)

    # Shut down control socket, to avoid spewing a lot of resource warnings when debugging
    ctl_server_task.cancel()
    await asyncio.wait({ctl_server_task, cmgr_task}, timeout = 60)
    # Also, leaving docker session open will spew warnings
    await icond.docker.close()


def start(params : argparse.Namespace):
    """ Entry point for module run """
    if not (params.force or os.geteuid() == 0):
        print("Starting of the ICON daemon might fail when not run as root...")
        print("Try --force if you are confident it will work")
        sys.exit(-1)
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    asyncio.run(main())
