import contextlib
import logging
from datetime import datetime
from itertools import count
from pickle import dumps, loads  # could be any arbitrary serialization method
from typing import Any, AsyncIterable, Optional

import anyio
import websockets
from anyio.abc import TaskStatus

from .common import create_context_async_generator
from .connection import Connection, wrap_websocket_connection
from .server import ClientSession

OK = "OK"
START_TASK = "Start task"
CLOSE_SENTINEL = "Done"
SHUTDOWN = "Shutdown"
CLOSE_STREAM = "Close stream"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"

STREAM_BUFFER = 100


class AsyncClientCore:
    """Implements non functional specific details."""

    def __init__(self, task_group, connection, name=None) -> None:
        self.task_group = task_group
        self.connection = connection
        self.request_id = count()
        self.pending_requests = {}
        self.name = name

    async def send(self, *args):
        await self.connection.send(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        await self.send(self.name)
        task_status.started()
        with contextlib.suppress(websockets.exceptions.ConnectionClosedOK):
            while True:
                message = await self.connection.recv()
                if message is None:
                    logging.info("Connection closed by the server.")
                    self.task_group.cancel_scope.cancel()
                    break
                request_id, success, result = message
                sink = self.pending_requests.get(request_id)
                if sink:
                    sink.send_nowait((success, result))

    async def send_command(self, command, args):
        request_id = next(self.request_id)
        sink, stream = anyio.create_memory_object_stream()
        try:
            self.pending_requests[request_id] = sink
            await self.send(request_id, command.__name__, args)
            success, result = await stream.receive()
            if not success:
                raise Exception(result)
            return result
        finally:
            self.pending_requests.pop(request_id)

    async def create_stream(self, return_sink, command, args, process_value):
        request_id = next(self.request_id)
        request_sink, request_stream = anyio.create_memory_object_stream(STREAM_BUFFER)
        try:
            self.pending_requests[request_id] = request_sink
            await self.send(request_id, command.__name__, args)
            while True:
                success, value = await request_stream.receive()
                if not success:
                    raise Exception(value)
                if value is None:
                    break
                await return_sink(process_value(*value) if process_value else value)
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(request_id, None, None)
                self.pending_requests.pop(request_id)

    def subscribe_stream(self, command, args, process_value=None):
        return create_context_async_generator(
            self.create_stream, command, args, process_value
        )


@contextlib.asynccontextmanager
async def _create_async_client_core(
    task_group, connection: Connection, name: str
) -> AsyncIterable[AsyncClientCore]:
    client = AsyncClientCore(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client


class AsyncClient:
    def __init__(
        self, client: AsyncClientCore, serializer=None, deserializer=None
    ) -> None:
        self.client = client
        self.serializer = serializer or dumps
        self.deserializer = deserializer or loads

    def read_events(
        self,
        topic: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        time_stamps_only: bool = False,
    ):
        if time_stamps_only:
            process_value = None
        else:
            process_value = lambda time_stamp, value: (
                time_stamp,
                loads(value),
            )
        return self.client.subscribe_stream(
            ClientSession.read_events,
            (topic, start, end, time_stamps_only),
            process_value,
        )

    async def write_event(
        self,
        topic: str,
        event: Any,
        time_stamp: Optional[datetime] = None,
        override: bool = False,
    ) -> datetime:
        return await self.client.send_command(
            ClientSession.write_event,
            (topic, self.serializer(event), time_stamp, override),
        )

    async def read_event(self, topic: str, time_stamp: datetime):
        return self.deserializer(
            await self.client.send_command(
                ClientSession.read_event, (topic, time_stamp)
            )
        )


@contextlib.asynccontextmanager
async def _create_async_client(
    task_group, connection: Connection, name: str
) -> AsyncIterable[AsyncClient]:
    async with _create_async_client_core(task_group, connection, name) as client:
        yield AsyncClient(client)


@contextlib.asynccontextmanager
async def create_async_client(
    host_name: str, port: str, name: str
) -> AsyncIterable[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with wrap_websocket_connection(host_name, port) as connection:
            async with _create_async_client(task_group, connection, name) as client:
                yield client
