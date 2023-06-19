import contextlib
import struct
from asyncio.streams import StreamReader, StreamWriter, open_connection
import sys

if sys.version_info < (3, 8):
    from asyncio.streams import IncompleteReadError
else:
    from asyncio.exceptions import IncompleteReadError
from pickle import dumps, loads
from typing import Any, Tuple, Callable

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
        self.serialize: Callable = serialize
        self.deserialize: Callable = deserialize
        self.throw_on_eof = throw_on_eof
        self._closing = False

    def send_nowait(self, message: Tuple[Any, ...]):
        message_as_bytes = self.serialize(message)
        self.writer.write(struct.pack(FORMAT, len(message_as_bytes)) + message_as_bytes)

    async def drain(self):
        await self.writer.drain()

    async def send(self, message: Tuple[Any, ...]):
        self.send_nowait(message)
        try:
            await self.writer.drain()
        except ConnectionResetError:
            if self.throw_on_eof:
                raise

    def close(self):
        self._closing = True
        self.writer.close()
        self.reader.feed_eof()

    async def __anext__(self):
        try:
            length = await self.reader.readexactly(SIZE_LENGTH)
            return self.deserialize(
                await self.reader.readexactly(struct.unpack(FORMAT, length)[0])
            )
        except (IncompleteReadError, ConnectionResetError, BrokenPipeError):
            if self.throw_on_eof:
                if not self._closing and self.reader.at_eof():
                    raise RuntimeError("Connection closed.")
                raise
            else:
                raise StopAsyncIteration()

    def __aiter__(self):
        return self

    async def aclose(self):
        self._closing = True
        self.writer.close()
        await self.wait_closed()

    async def wait_closed(self):
        await self.writer.wait_closed()


@contextlib.asynccontextmanager
async def connect_to_tcp_server(host_name: str, port: int, serialize=dumps, deserialize=loads):
    reader, writer = await open_connection(host_name, port)
    connection = TCPConnection(reader, writer, False, deserialize, serialize)
    async with asyncstdlib.closing(connection):
        yield connection
