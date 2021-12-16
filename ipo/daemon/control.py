"""
The control channel is used for communication with the command line interface.
"""
import asyncio
import os
import logging

from . state import Icond
from . messagetask import MessageTaskDispatcher
from . ctltask import CTL_HANDLERS


log = logging.getLogger(__name__)


class ControlServer:
    """ Control socket server """
    icond: Icond

    def __init__(self, icond: Icond):
        self.icond = icond

    async def _connection_handler(self, reader, writer):
        async with MessageTaskDispatcher(reader, writer, CTL_HANDLERS, self.icond) as dispatcher:
            async for unhandled in dispatcher:
                log.error('Unhandled %s', unhandled)

    async def run(self):
        """ Run server """
        control_socket = self.icond.config.control_socket
        try:
            os.unlink(control_socket)
        except OSError:
            if os.path.exists(control_socket):
                raise

        server = await asyncio.start_unix_server(
            self._connection_handler,
            path = control_socket
        )
        async with server:
            await server.serve_forever()
