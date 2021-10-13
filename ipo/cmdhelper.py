"""
Helpers for communicating commands to the daemon
"""
import asyncio
from typing import Union

from .connection import Connection
from .api import message


def send_and_receive(msg: message.IconMessage, mask_exceptions = True):
    """
    One shot synchornous send-receive handler for control messages.

    mask_exceptions:    Catch exceptions and print error messages accordingly
    """
    async def do_async():
        conn = await Connection.connect()
        await conn.write(msg)
        return await conn.read()
    exc = None  # type: Union[OSError, None]
    try:
        return asyncio.run(do_async())
    except PermissionError as e:
        exc = e
        print('Permission denied when communicating with daemon')
    except OSError as e:
        exc = e
        print(f'Error {e} when communicating with daemon')
    if not mask_exceptions and exc is not None:
        raise exc
    return None
