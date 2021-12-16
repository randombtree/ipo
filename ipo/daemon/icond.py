"""
ICOND server; starting and stopping.
"""

import sys
import asyncio
import os
import argparse
import logging

import docker  # type: ignore
from setproctitle import setproctitle

from . events import ShutdownEvent
from . state import Icond
from ..util.asynctask import waitany
from . signals import set_signal_handlers
from . control import ControlServer


log = logging.getLogger(__name__)


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
    set_signal_handlers(icond)
    await icond.run()


def start(params : argparse.Namespace):
    """ Entry point for module run """
    if not (params.force or os.geteuid() == 0):
        print("Starting of the ICON daemon might fail when not run as root...")
        print("Try --force if you are confident it will work")
        sys.exit(-1)
    setproctitle('ipo_server')
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    asyncio.run(main())
