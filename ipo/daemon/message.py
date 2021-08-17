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
    FIELD_TYPE = "type"
    FIELD_ID   = "id"
    TYPE_COMMAND = "command"
    TYPE_ERROR = "error"

    def __init__(self, msg_type: str = TYPE_COMMAND, msg_id = None, **data):
        """
        msg_type: The message type
        msg_id: A message ID to use (in reply messages), othervise assign a new unique id.
        data: The rest of the message fields.
        """

        self.msg_type = msg_type
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

    def as_dict(self):
        """ Output message as dict data, for later feeding to json """
        d = {
            self.FIELD_TYPE: self.msg_type,
            self.FIELD_ID: str(self.msg_id),
            **self.data
        }
        return d

    @classmethod
    def from_dict(cls, source: dict):
        """ Re-construct message from dictionary data """
        if not (cls.FIELD_TYPE in source and cls.FIELD_ID in source):
            raise InvalidMessage("missing in message: %s %s" % (
                                 "type " if not cls.FIELD_TYPE else "",
                                 "id " if not cls.FIELD_ID else ""))
        msg_type = source[cls.FIELD_TYPE]
        msg_id = source[cls.FIELD_ID]
        return IconMessage(msg_type, msg_id = msg_id, **source)


class JSONWriter:
    """ Simple JSON wrapper over StreamWriter """
    writer: asyncio.StreamWriter

    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    async def write(self, data: Union[dict, IconMessage]):
        """ Write message as JSON to backend """
        if isinstance(data, IconMessage):
            data = data.as_dict()
        s = json.dumps(data)
        self.writer.write(s.encode())
        await self.writer.drain()
