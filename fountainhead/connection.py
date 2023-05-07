import contextlib

from msgpack import dumps, loads
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK


class Connection:
    def __init__(self, websocket) -> None:
        self.websocket = websocket

    async def send(self, message):
        with contextlib.suppress(ConnectionClosedError, ConnectionClosedOK):
            return await self.websocket.send(dumps(message))

    async def recv(self):
        with contextlib.suppress(ConnectionClosedError, ConnectionClosedOK):
            return loads(await self.websocket.recv())

    async def __aiter__(self):
        with contextlib.suppress(ConnectionClosedError, ConnectionClosedOK):
            async for message in self.websocket:
                yield loads(message)

    async def close(self):
        await self.websocket.close()
