import contextlib
import struct
from asyncio.streams import IncompleteReadError, StreamReader, StreamWriter, open_connection
from pickle import dumps, loads
from typing import Any, Tuple

import asyncstdlib

from .abc import Connection


FORMAT = "Q"
SIZE_LENGTH = 8


class TCPConnection(Connection):
    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        throw_on_eof=True,
        deserialize=loads,
        serialize=dumps,
    ):
        self.reader = reader
        self.writer = writer
        self.serialize = serialize
        self.deserialize = deserialize
        self.throw_on_eof = throw_on_eof
        self._closing = False

    async def send(self, message: Tuple[Any, ...]):
        message_as_bytes = self.serialize(message)
        self.writer.write(struct.pack(FORMAT, len(message_as_bytes)) + message_as_bytes)
        await self.writer.drain()

    async def recv(self) -> Any:
        try:
            length = await self.reader.readexactly(SIZE_LENGTH)
            return self.deserialize(
                await self.reader.readexactly(struct.unpack(FORMAT, length)[0])
            )
        except (IncompleteReadError, ConnectionResetError):
            if not self._closing and self.reader.at_eof() and self.throw_on_eof:
                raise RuntimeError("Connection closed.")
            raise

    async def __anext__(self):
        return await self.recv()

    async def __aiter__(self):
        while True:
            yield await self.recv()

    async def aclose(self):
        self._closing = True
        self.writer.close()
        await self.wait_closed()

    async def wait_closed(self):
        await self.writer.wait_closed()


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int, serialize=dumps, deserialize=loads):
    reader, writer = await open_connection(host_name, port)
    connection = TCPConnection(reader, writer, True, deserialize, serialize)
    async with asyncstdlib.closing(connection):
        yield connection
