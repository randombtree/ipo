"""
ICOND server; starting and stopping.
"""

import sys
import asyncio
import os
import argparse
import logging

import docker  # type: ignore

from . events import ShutdownEvent
from . state import Icond
from . messagetask import MessageTaskDispatcher
from . ctltask import CTL_HANDLERS
from ..util.asynctask import waitany
from . signals import set_signal_handlers


log = logging.getLogger(__name__)


async def iconctl_connection_handler(reader, writer, icond: Icond):
    """ Icon control channel handler """
    async with MessageTaskDispatcher(reader, writer, CTL_HANDLERS, icond) as dispatcher:
        async for unhandled in dispatcher:
            log.error('Unhandled %s', unhandled)


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
        try:
            await icond.docker.containers.run(
                "registry:2",
                name=repository,
                detach=True,
                restart_policy={"name": "always"},
                ports={"5000/tcp": 5000}       # TODO: Config
            )
        except docker.errors.APIError as e:
            # Ok, this happened once (in VM w/o proper connection), so better
            # deal with it to not cause more head-scratchers
            log.error('Failed to initialize registry. Possible cause; internet connection? %s', e)
            raise InitializationException('Failed to initialize registry') from e
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
    await icond.start()
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
    await icond.stop()
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
