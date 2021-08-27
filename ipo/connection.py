"""
Client connection to ipo

"""
import asyncio

from .daemon import message


class Connection:
    """
    Async connection to ICON daemon.
    """
    reader: message.MessageReader
    writer: message.JSONWriter

    def __init__(self, reader: message.MessageReader, writer: message.JSONWriter):
        """
        path: The unix socket that the ICON daemon listens to.

        Exceptions: Fails if daemon isn't started.
        """
        self.reader = reader
        self.writer = writer

    # FIXME: Paths and stuff might be configurable or at least define them in the same file
    @classmethod
    async def connect(cls, path = "/var/run/icond/icond.sock"):
        """ Connect to Unix socket """
        (reader, writer) = await asyncio.open_unix_connection(path)
        reader = message.MessageReader(reader)
        writer = message.JSONWriter(writer)
        return cls(reader, writer)

    async def write(self, msg: message.IconMessage):
        """ Write a message to the daemon """
        return await self.writer.write(msg)

    async def read(self) -> message.IconMessage:
        """ Wait for a message from the daemon """
        return await self.reader.read()
