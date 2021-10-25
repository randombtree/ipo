"""
iconsrv messages
"""
import aiohttp
from aiohttp import web

import logging

from ipo.api.message import (
    IconMessage,
    InvalidMessage,
)


log = logging.getLogger(__name__)


class UserHelloReply(IconMessage):
    """ Handshake reply from server """
    FIELD_VALIDATORS = dict(session_id = str)


class UserHello(IconMessage):
    """ Connection initiation message from client """
    REPLY_CLS = UserHelloReply


class MessageSocket:
    """ ICON messaging over WS """
    ws: web.WebSocketResponse

    def __init__(self, ws: web.WebSocketResponse):
        self.ws = ws

    async def receive(self) -> IconMessage:
        """ Receive a message from ws """
        msg = await self.ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            return IconMessage.from_json(msg.data)
        elif msg.type == aiohttp.WSMsgType.ERROR:
            log.warning('ws connection closed with exception: %s', self.ws.exception())
            raise InvalidMessage('ws connection error') from self.ws.exception()
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            # FIXME: Use another exception type perhaps
            raise InvalidMessage('ws connection closed')

        raise InvalidMessage(f'unhandled message {msg}')

    async def send(self, msg: IconMessage):
        """ Send message """
        await self.ws.send_json(msg.as_dict())

