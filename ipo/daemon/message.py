""" Icond messaging """
import uuid
import json
import asyncio
from typing import Union


class JSONReader:
    """ Simple JSON wrapper over StreamReader """
    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def read(self):
        """ Read a single JSON message from backend """
        line = (await self.reader.readline()).decode()
        return json.loads(line)


class InvalidMessage(Exception):
    """ Message is invalid somehow """


class IconMessage:
    """ Icon Control message """
    # Common fields (dictionary keys) used in messages
    FIELD_ID      = "id"
    FIELD_TYPE    = "type"
    FIELD_COMMAND = "command"

    # FIELD_TYPE can be one of the following:
    TYPE_COMMAND = "command"
    TYPE_REPLY = "reply"
    TYPE_ERROR = "error"

    # FIELD_COMMAND can me one of the following (when appropriable)
    COMMAND_SHUTDOWN = "shutdown"
    COMMAND_CONTAINER_RUN = 'container run'

    def __init__(self, msg_type: str = TYPE_COMMAND, msg_id = None, **data):
        """
        msg_type: The message type
        msg_id: A message ID to use (in reply messages), othervise assign a new unique id.
        data: The rest of the message fields.
        """

        self.msg_type = msg_type
        # Allow msg_id to be supplied from the data; this usually helps when de-serializing data
        if msg_id is None and self.FIELD_ID in data:
            msg_id = data[self.FIELD_ID]
        # create uuid from different types; n.b. UUID objects are immutable
        self.msg_id = \
            uuid.UUID(msg_id) if isinstance(msg_id, str) \
            else uuid.UUID(msg_id['id']) if isinstance(msg_id, dict) \
            else msg_id if isinstance(msg_id, uuid.UUID) \
            else uuid.uuid4()  # Swallow erronous msg_id here for simplicity
        self.data = data.copy()
        # Don't carry these in data
        for k in [self.FIELD_TYPE, self.FIELD_ID]:
            self.data.pop(k, None)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getattr__(self, key):
        """ Convinience method for accessing message fields directly as msg.field """
        if key not in self.data:
            raise AttributeError(f'Field {key} not present in message')
        return self.data[key]

    def as_dict(self):
        """ Output message as dict data, for later feeding to json """
        d = {
            self.FIELD_TYPE: self.msg_type,
            self.FIELD_ID: str(self.msg_id),
            **self.data
        }
        return d

    def create_reply(self, **data) -> 'Reply':
        """ Create a reply message based on this message (i.e. copy id) """
        return Reply(msg_id = self.msg_id, **data)

    @classmethod
    def from_dict(cls, source: dict) -> 'IconMessage':
        """ Re-construct message from dictionary data """
        if not (cls.FIELD_TYPE in source and cls.FIELD_ID in source):
            raise InvalidMessage("missing in message: %s %s" % (
                                 "type " if not cls.FIELD_TYPE else "",
                                 "id " if not cls.FIELD_ID else ""))
        msg_type = source.pop(cls.FIELD_TYPE)
        msg_id = source.pop(cls.FIELD_ID)
        # Parse convinience classes
        if msg_type == cls.TYPE_COMMAND:
            msg_command = source.pop(cls.FIELD_COMMAND)
            msg_cls = \
                Shutdown if msg_command == cls.COMMAND_SHUTDOWN \
                else ContainerRun if msg_command == cls.COMMAND_CONTAINER_RUN \
                else None
            if msg_cls is None:
                raise InvalidMessage(f'Unhandled command {msg_command}')
            return msg_cls(msg_id = msg_id, **source)
        return IconMessage(msg_type, msg_id = msg_id, **source)

    def __str__(self):
        return f'({self.msg_type}, {self.msg_id}) {self.data}'


class Shutdown(IconMessage):
    """ Convinience class for a Shutdown message """
    def __init__(self, **kvargs):
        super().__init__(msg_type = IconMessage.TYPE_COMMAND, command = IconMessage.COMMAND_SHUTDOWN, **kvargs)

class ContainerRun(IconMessage):
    ARG_IMAGE = 'image'
    """ Container run command """
    def __init__(self, **kvargs):
        if ContainerRun.ARG_IMAGE not in kvargs:
            raise InvalidMessage('missing image argument')
        super().__init__(msg_type = IconMessage.TYPE_COMMAND, command = IconMessage.COMMAND_CONTAINER_RUN, **kvargs)


class Reply(IconMessage):
    """ Convinience class for a simple reply message """
    def __init__(self, msg_id, **data):
        super().__init__(msg_type = IconMessage.TYPE_REPLY, msg_id = msg_id, **data)


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
        print(s)
        self.writer.write(s.encode())
        await self.writer.drain()


class MessageReader(JSONReader):
    """ Reader that translates json chunks to ICON messages """
    async def read(self) -> IconMessage:
        """ Read next ICON message; might throw if message is malformed """
        msg = await super().read()
        return IconMessage.from_dict(msg)
