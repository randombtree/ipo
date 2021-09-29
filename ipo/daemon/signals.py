""" ICON daemon signal handlers """
import asyncio
import signal

from . state import Icond


class QuitSignalHandler:
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

    def _print_help(self):
        """ Print some help info in daemon in reponse to signal """
        # pylint: disable=no-self-use
        ...


class SigintHandler(QuitSignalHandler):
    """ Handle interrupt from keyboard """
    # pylint: disable=too-few-public-methods
    def _print_help(self):
        if self.counter == 1:
            print("CTRL-C pressed - shutdown")
        elif self.counter == 2:
            print("Press CTRL-C one more time to forcibly kill")


class SigtermHandler(QuitSignalHandler):
    """ Handle SIGTERM (kill) signal """
    # pylint: disable=too-few-public-methods
    def __init__(self, icond: Icond):
        super().__init__(icond, 2)  # Two SIGTERMs = kill

    def _print_help(self):
        print("SIGTERM received")


def set_signal_handlers(icond: Icond):
    """ Hook up some common signal handlers """
    loop = asyncio.get_event_loop()
    # Allow quitting with CTRL-C and kill
    loop.add_signal_handler(signal.SIGINT, SigintHandler(icond).handle)
    loop.add_signal_handler(signal.SIGTERM, SigtermHandler(icond).handle)
