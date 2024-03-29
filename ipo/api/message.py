""" Icond messaging """
import uuid
import json
import asyncio
from typing import Union, Optional
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

        # Make sure to not register duplicate message names, it leads to
        # "interesting problems".
        if name in cls.registered:
            raise Exception(f'{name} message is already registered!')
        # store it
        cls.registered[name] = newtype
        return newtype

    @classmethod
    def get_message_class(cls, name):
        """ Get the message class for name """
        if name in cls.registered:
            return cls.registered[name]
        raise MessageTypeNotFoundException(f'{name} is not a valid message')


IconId = uuid.UUID   # ICON message id type


class IconMessage(metaclass = MessageRegistry):
    """ Icon message base / Factory """
    # Avoid using naked string literals for message fields
    STR_ID      = "id"
    STR_TYPE    = "type"

    FIELD_VALIDATORS = dict()  # type: dict[str, Union[None, Callable[[str], bool], type]]
    REPLY_CLS = 'Reply'        # type: Union[None, str, type]  # Create a reply message using this class

    data: dict[str, str]
    msg_id: IconId

    def __init__(self, *, msg_id = None, **data):
        self.data = data.copy()

        self.msg_id = \
            uuid.UUID(msg_id) if isinstance(msg_id, str) \
            else uuid.UUID(msg_id['id']) if isinstance(msg_id, dict) \
            else msg_id if isinstance(msg_id, uuid.UUID) \
            else IconMessage.gen_id()  # Swallow erronous msg_id here for simplicity

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
                        try:
                            self.data[field] = validator(value)
                        except ValueError as e:
                            raise InvalidMessage(f'{clsname} field {field} of invalid type {vtype} ({value}), expected {validator}') from e
                elif not validator(value):
                    raise InvalidMessage(f'{clsname} field {field} of invalid value {value}')

    @staticmethod
    def gen_id() -> IconId:
        """ Generate new uuid """
        return uuid.uuid4()

    @staticmethod
    def id_generator():
        """ Generate a continous list of message id's """
        while True:
            yield IconMessage.gen_id()

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

    def as_json(self):
        """ Return message data as json string """
        d = self.as_dict()
        return json.dumps(d)

    def create_reply(self, /, reply_cls: Optional[type['Reply']] = None, **data) -> 'Reply':
        """ Create a reply message based on this message (i.e. copy id) """
        # NB: Will throw is self hasn't got a reply class
        if reply_cls is None:
            reply_cls = self.REPLY_CLS if isinstance(self.REPLY_CLS, type) \
                else MessageRegistry.get_message_class(self.REPLY_CLS)
        return reply_cls(msg_id = self.msg_id, **data)

    @classmethod
    def reply_to(cls, reply_msg: 'IconMessage', **kwargs) -> 'IconMessage':
        """ Create a reply message to an existing message """
        return cls(msg_id = reply_msg.msg_id, **kwargs)

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

    @classmethod
    def from_json(cls, source: str) -> 'IconMessage':
        """ De-serialize from json """
        return cls.from_dict(json.loads(source))

    @classmethod
    def match(cls, msg: 'IconMessage') -> bool:
        """ Quick hand for checking if msg is of our type """
        return isinstance(msg, cls)

    def __str__(self):
        return f'{self.__class__.__name__}: {self.msg_id} {self.data}'


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


class OrchestratorHelloReply(Reply):
    """ Reply message to Hello """
    FIELD_VALIDATORS = dict(version = None, ip = str, port = int)
    ...


class OrchestratorHello(IconMessage):
    """ Hello to orchestrator """
    FIELD_VALIDATORS = dict(version = None, ip = str, port = int)
    REPLY_CLS = OrchestratorHelloReply


class MigrationResponse(IconMessage):
    """ Migration completed """
    # Returns the IP address and port mappings for the new deployment.
    FIELD_VALIDATORS = dict(ip = str, ports = dict)


class MigrationRequest(IconMessage):
    """ Ask for migration """
    FIELD_VALIDATORS = dict(image_name = str, ports = list, environment = dict)


class ShutdownCommand(IconMessage):
    """ Send a command to remote """
    REPLY_CLS = ReplyMsg


class DeploymentStateChanged(IconMessage):
    """ Send new deployment state """
    FIELD_VALIDATORS = dict(state = str)


class ClientHello(IconMessage):
    """ ICON container initialization message """
    FIELD_VALIDATORS = dict(version = None)
    REPLY_CLS = HelloReply
    ...


class ClientMigrate(IconMessage):
    """ ICON Container migration request """
    FIELD_VALIDATORS = dict(ip = str, port = int)
    REPLY_CLS = ReplyMsg
    ...


class ClientDeploymentStatus(IconMessage):
    """ Update event from daemon to client on deployment """
    FIELD_VALIDATORS = dict(state = str, ip = str, ports = dict)


class BootstrapNode(IconMessage):
    """ Provide DHT bootstrap node """
    FIELD_VALIDATORS = dict(ip = str, port = int)
    REPLY_CLS = ReplyMsg
    ...


class OrchestratorListing(Reply):
    """ Listing for orchestrators """
    FIELD_VALIDATORS = dict(metrics = list)


class FindOrchestrator(IconMessage):
    """ Find orchestrator for ip """
    FIELD_VALIDATORS = dict(ip = str)
    REPLY_CLS = OrchestratorListing


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
