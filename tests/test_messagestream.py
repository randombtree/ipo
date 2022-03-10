"""
Test ipo.util.messagestream
"""
from unittest import IsolatedAsyncioTestCase
import asyncio

from ipo.api import message
from ipo.api.message import IconMessage
from ipo.util.messagestream import (
    MessageStreamHandler,
    MessageStreamManager,
    StreamMessageEvent,
)


class DummyOutputStream:
    """ Output stream that just records the sent messages """
    messages: list[IconMessage]

    def __init__(self):
        self.messages = []

    async def send(self, msg: IconMessage):
        self.messages.append(msg)


class TimeoutException(IOError):
    """ Emitted by dummy input stream on timeout """
    ...


class DummyInputStream:
    """ Message generator """
    timeout: int
    messages: list[IconMessage]
    ndx: int

    def __init__(self, messages: list[IconMessage] = [], timeout: int = 1):
        self.messages = messages
        self.timeout = timeout
        self.ndx = 0

    async def receive(self) -> IconMessage:
        """ Send message / timeout after a while """
        if self.ndx >= len(self.messages):
            await asyncio.sleep(self.timeout)
            raise TimeoutException('Timeout')

        msg = self.messages[self.ndx]
        self.ndx += 1
        return msg

    def close(self):
        """ Close placeholder """
        ...


class DummyIncomingHandler(MessageStreamHandler):
    """ Handle incoming messages and record them """
    incoming_messages: list
    shutdown_triggers: list

    def handle_incoming(self, event):
        """ Process incoming messages """
        if isinstance(event, StreamMessageEvent):
            self.incoming_messages.append(event.msg)

    def handle_shutdown(self):
        """ Record shutdown """
        self.shutdown_triggers.append(True)

    async def run(self):
        async for event in self:
            self.handle_incoming(event)
        self.handle_shutdown()


class DummySendingHandler(MessageStreamHandler):
    """ Simple message sending handler """
    send_msg: list[type[IconMessage]]

    async def run(self):
        for msg in self.send_msg:
            await self._send(msg)


class TestMessageStream(IsolatedAsyncioTestCase):
    """ Test message stream manager """
    @staticmethod
    def _create_dummy(timeout: int = 1):
        """ Create dummy manager """
        out_stream = DummyOutputStream()
        in_stream = DummyInputStream(timeout = timeout)
        return MessageStreamManager.handle(in_stream, out_stream)

    async def test_dummy(self):
        """ Test dummy creation """
        uh_list = []
        async with self._create_dummy(timeout = 0) as manager:
            async for unhandled in manager:
                uh_list.append(unhandled)
        self.assertEqual(len(uh_list), 0, 'Unhandled events?')

    async def test_receive(self):
        """ Test receiving messages """
        incoming_messages = []
        shutdown_triggers = []
        out_stream = DummyOutputStream()
        in_stream = DummyInputStream(messages = [message.ShutdownCommand()], timeout = 1)
        params = dict(incoming_messages = incoming_messages,
                      shutdown_triggers = shutdown_triggers)
        uh_list = []
        async with MessageStreamManager.handle(in_stream, out_stream, **params) as manager:
            manager.add_handlers({message.ShutdownCommand: DummyIncomingHandler})
            async for unhandled in manager:
                uh_list.append(unhandled)
        self.assertEqual(len(uh_list), 0, 'Unhandled events?')

        self.assertEqual(len(incoming_messages), 1)
        self.assertEqual(len(shutdown_triggers), 1)
        first_msg = incoming_messages.pop()
        self.assertIs(type(first_msg), message.ShutdownCommand)

    async def test_send(self):
        """ Test sending via new session """
        out_stream = DummyOutputStream()
        in_stream = DummyInputStream()
        uh_list = []
        async with MessageStreamManager.handle(in_stream, out_stream) as manager:
            handler = manager.new_session(DummySendingHandler, send_msg = [message.ShutdownCommand])
            async for unhandled in manager:
                uh_list.append(unhandled)
        self.assertEqual(len(uh_list), 0, 'Unhandled events?')
        self.assertIs(type(handler), DummySendingHandler)
        self.assertEqual(len(out_stream.messages), 1)
        first_msg = out_stream.messages[0]
        self.assertIs(type(first_msg), message.ShutdownCommand)
