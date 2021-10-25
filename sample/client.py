#!/usr/bin/env python3.10
"""
ICON sample client application
"""
import sys
import asyncio

import aiohttp

from iconsrv.message import Hello, HelloReply, MessageSocket
from ipo.util.asynctask import AsyncTaskRunner


async def main(argv):
    """
    Client stub.

    Connect to server websocket and do handshake.
    """
    host = argv[1]
    session = aiohttp.ClientSession()
    runner = AsyncTaskRunner()
    async with session.ws_connect(f'http://{host}/ws') as ws:
        print('connected')
        ms = MessageSocket(ws)

        # Handshake
        await ms.send(Hello())
        msg = await ms.receive()
        if not isinstance(msg, HelloReply):
            print('Handshake error')
            return
        ws_read_task = runner.run(ms.receive)
        async for task in runner.wait_next():
            if task == ws_read_task:
                exc = task.exception()
                if exc:
                    print(f'Exception {exc} receiving messages')
                    break
                msg = task.result()
                print(msg)
    await session.close()


if __name__ == '__main__':
    asyncio.run(main(sys.argv))
