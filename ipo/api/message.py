""" Icond messaging """
import uuid
import json
import asyncio
from typing import Union
from collections.abc import Callable

# pylint: disable=too-few-public-methods


class JSONReader:
    """ Simple JSON wrapper over StreamReader """
    reader: asyncio.StreamReader

    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def read(self):
        """ Read a single JSON message from backend """
        line = (await self.reader.readline()).decode()
        return json.loads(line)

    def close(self):
        """ Close the underlying stream """
        # The API is slightly confusing as the reader lacks a close
        # but the writer has one; try to bring some sanity into this
        # by providing a blank close here
        return


class InvalidMessage(Exception):
    """ Message is invalid somehow """
    ...


class MessageTypeNotFoundException(Exception):
    """ Message type is invalid/not implemented """
    ...


class MessageRegistry(type):
    """ Meta class that keeps track of all registered messages """
    registered = dict()  # type: dict[str, type]   #Register all classes here

    def __new__(cls, name, bases, attrs):
        # create the new type
        newtype = super(MessageRegistry, cls).__new__(cls, name, bases, attrs)
        # store it
        cls.registered[name] = newtype
        return newtype

    @classmethod
    def get_message_class(cls, name):
        """ Get the message class for name """
        if name in cls.registered:
            return cls.registered[name]
        raise MessageTypeNotFoundException(f'{name} is not a valid message')


class IconMessage(metaclass = MessageRegistry):
    """ Icon message base / Factory """
    # Avoid using naked string literals for message fields
    STR_ID      = "id"
    STR_TYPE    = "type"

    FIELD_VALIDATORS = dict()  # type: dict[str, Union[None, Callable[[str], bool], type]]
    REPLY_CLS = 'Reply'        # type: Union[None, str, type]  # Create a reply message using this class

    data: dict[str, str]
    msg_id: uuid.UUID

    def __init__(self, /, msg_id = None, **data):
        self.data = data.copy()

        self.msg_id = \
            uuid.UUID(msg_id) if isinstance(msg_id, str) \
            else uuid.UUID(msg_id['id']) if isinstance(msg_id, dict) \
            else msg_id if isinstance(msg_id, uuid.UUID) \
            else uuid.uuid4()  # Swallow erronous msg_id here for simplicity

        # Validate fields
        for field, validator in self.FIELD_VALIDATORS.items():
            clsname = self.__class__.__name__
            if field not in self.data:
                raise InvalidMessage(f'{clsname} requires field {field}')
            value = self.data[field]
            # TODO: Validators should only run when in developer mode
            if validator is not None:
                # Validator can either be a type or a callable
                if isinstance(validator, type):
                    if not isinstance(value, validator):
                        vtype = type(value)
                        raise InvalidMessage(f'{clsname} field {field} of invalid type {vtype}, expected {validator}')
                elif not validator(value):
                    raise InvalidMessage(f'{clsname} field {field} of invalid value {value}')

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __contains__(self, key):
        return key in self.data

    def __getattr__(self, key):
        """ Convinience method for accessing message fields directly as msg.field """
        if key not in self.data:
            raise AttributeError(f'Field {key} not present in message')
        return self.data[key]

    def as_dict(self):
        """ Output message as dict data, for later feeding to json """
        d = {
            self.STR_TYPE: self.__class__.__name__,
            self.STR_ID: str(self.msg_id),
            **self.data
        }
        return d

    def create_reply(self, **data) -> 'Reply':
        """ Create a reply message based on this message (i.e. copy id) """
        # NB: Will throw is self hasn't got a reply class
        reply_cls = self.REPLY_CLS if isinstance(self.REPLY_CLS, type) \
            else MessageRegistry.get_message_class(self.REPLY_CLS)
        return reply_cls(msg_id = self.msg_id, **data)

    @classmethod
    def from_dict(cls, source: dict) -> 'IconMessage':
        """ De-serialize class from dict """
        if not (cls.STR_TYPE in source and cls.STR_ID in source):
            raise InvalidMessage("missing in message: %s %s" % (
                                 "type " if not cls.STR_TYPE else "",
                                 "id " if not cls.STR_ID else ""))

        msg_type = source.pop(cls.STR_TYPE)
        msg_id = source.pop(cls.STR_ID)
        try:
            msg_cls = MessageRegistry.get_message_class(msg_type)
        except MessageTypeNotFoundException as e:
            raise InvalidMessage(f'Unknown message type {msg_type}') from e
        return msg_cls(msg_id = msg_id, **source)


class Reply(IconMessage):
    """ Reply message """
    REPLY_CLS = None
    ...


class ReplyMsg(Reply):
    """ Reply with obligatory message """
    FIELD_VALIDATORS = dict(msg = None)


class Error(ReplyMsg):
    """ Error message """
    ...


class Shutdown(IconMessage):
    """ Shutdown message """
    REPLY_CLS = ReplyMsg
    ...


class ContainerRun(IconMessage):
    """ Run container message """
    FIELD_VALIDATORS = dict(image = None)
    ...


class ContainerListing(Reply):
    """ Listing of containers """
    # TODO: Proper validator for containers listing
    FIELD_VALIDATORS = dict(containers = dict)
    ...


class ContainerLs(IconMessage):
    """ List containers cmd """
    REPLY_CLS = ContainerListing
    ...


class HelloReply(IconMessage):
    """ Reply message to *Hello """
    FIELD_VALIDATORS = dict(version = None)
    ...


class ClientHello(IconMessage):
    """ ICON container initialization message """
    FIELD_VALIDATORS = dict(version = None)
    REPLY_CLS = HelloReply
    ...


class JSONWriter:
    """ Simple JSON wrapper over StreamWriter """
    writer: asyncio.StreamWriter

    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    async def write(self, data: Union[dict, IconMessage]):
        """ Write message as JSON to backend """
        if isinstance(data, IconMessage):
            data = data.as_dict()
        s = json.dumps(data) + '\n'
        self.writer.write(s.encode())
        await self.writer.drain()

    def close(self):
        """ Close the underlying stream """
        self.writer.close()


class MessageReader(JSONReader):
    """ Reader that translates json chunks to ICON messages """
    async def read(self) -> IconMessage:
        """ Read next ICON message; might throw if message is malformed """
        msg = await super().read()
        return IconMessage.from_dict(msg)
