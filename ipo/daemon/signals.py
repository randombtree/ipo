""" ICON daemon signal handlers """
import asyncio
import signal
import logging
import inspect
from abc import ABCMeta, abstractmethod

from . state import Icond


log = logging.getLogger(__name__)


class QuitSignalHandler(metaclass = ABCMeta):
    """ Handle termination signal """
    # pylint: disable=too-few-public-methods
    counter: int
    max_counter: int
    icond: Icond

    def __init__(self, icond: Icond, max_counter = 3):
        self.counter = 0
        self.icond = icond
        self.max_counter = max_counter

    def handle(self):
        """ This method should be passed to asyncio loop as signal handler """
        self.counter += 1
        self.icond.do_shutdown()
        self._print_help()
        if self.counter == self.max_counter:
            signal.raise_signal(signal.SIGKILL)

    @abstractmethod
    def _print_help(self):
        """ Print some help info in daemon in reponse to signal """
        # pylint: disable=no-self-use
        ...


class SigintHandler(QuitSignalHandler):
    """ Handle interrupt from keyboard """
    # pylint: disable=too-few-public-methods
    def _print_help(self):
        if self.counter == 1:
            print('\nCTRL-C pressed - shutdown')
        elif self.counter == 2:
            print('\nPress CTRL-C one more time to forcibly kill')
        log.info('Received SIGINT')


class SigtermHandler(QuitSignalHandler):
    """ Handle SIGTERM (kill) signal """
    # pylint: disable=too-few-public-methods
    def __init__(self, icond: Icond):
        super().__init__(icond, 2)  # Two SIGTERMs = kill

    def _print_help(self):
        log.info("SIGTERM received")


class TaskDumpHandler():
    """ Task dumpper handler """
    icond: Icond

    def __init__(self, icond: Icond):
        self.icond = icond

    def handle(self):
        """ Signal handler; print stack traces """
        current_task = asyncio.current_task()
        log.critical('Asyncio task dump:')

        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            frames = task.get_stack()
            for frame in frames:
                frameinfo = inspect.getframeinfo(frame)
                clist = frameinfo.code_context
                context = clist[0].strip() if clist else '<unknown>'
                log.critical('%s:%d at %s - %s',
                             frameinfo.filename,
                             frameinfo.lineno,
                             frameinfo.function,
                             context)

        log.critical('End of asyncio task dump')


def set_signal_handlers(icond: Icond):
    """ Hook up some common signal handlers """
    loop = asyncio.get_event_loop()
    # Allow quitting with CTRL-C and kill
    loop.add_signal_handler(signal.SIGINT, SigintHandler(icond).handle)
    loop.add_signal_handler(signal.SIGTERM, SigtermHandler(icond).handle)
    # Dump asyncio tasks on USR1
    loop.add_signal_handler(signal.SIGUSR1, TaskDumpHandler(icond).handle)
