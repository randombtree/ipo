"""
iconsrv messages
"""
from typing import Union
import aiohttp
from aiohttp import web

import logging

from ipo.api.message import (
    IconMessage,
    InvalidMessage,
    Reply,
)


log = logging.getLogger(__name__)


class UserHelloReply(IconMessage):
    """ Handshake reply from server """
    FIELD_VALIDATORS = dict(session_id = str)


class UserHello(IconMessage):
    """ Connection initiation message from client """
    REPLY_CLS = UserHelloReply


class UserMigrateReply(Reply):
    """ Reply to migrate message when user has successfully migrated """
    ...


class UserMigrate(IconMessage):
    """ Migration message """
    FIELD_VALIDATORS = dict(ip = str, port = int)
    REPLY_CLS = UserMigrateReply


class UserPayloadReply(Reply):
    """ Payload reply """
    FIELD_VALIDATORS = dict(data = str)


class UserPayloadMessage(IconMessage):
    """ Payload message """
    FIELD_VALIDATORS = dict(data = str)
    REPLY_CLS = UserMigrateReply


# aiohttp should really provide an interface class
WebSocketResponse = Union[aiohttp.ClientWebSocketResponse, web.WebSocketResponse]


class MessageSocket:
    """ ICON messaging over WS """
    ws: WebSocketResponse

    def __init__(self, ws: WebSocketResponse):
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

