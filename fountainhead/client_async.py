import contextlib
from datetime import datetime
import logging
import traceback
from itertools import count
from typing import Any, Optional, AsyncIterable

import anyio
from anyio.abc import TaskStatus
import websockets
from pickle import dumps, loads  # could be any arbitrary serialization method

from .common import create_context_async_generator
from .connection import Connection, wrap_websocket_connection

OK = "OK"
START_TASK = "Start task"
CLOSE_SENTINEL = "Done"
SHUTDOWN = "Shutdown"
CLOSE_STREAM = "Close stream"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"


class AsyncClientBase:
    """Implements non functional specific details"""

    def __init__(
        self, task_group, connection, serializer=None, deserializer=None, name=None
    ) -> None:
        super().__init__()
        self.task_group = task_group
        self.connection = connection
        self.serializer = serializer or dumps
        self.deserializer = deserializer or loads
        self.request_id = count()
        self.pending_requests = {}
        self.name = name

    async def close(self):
        logging.info("Closing connection.")
        await self.connection.close()
        self.task_group.cancel_scope.cancel()

    async def send(self, *args):
        await self.connection.send(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        await self.send(self.name)
        task_status.started()
        try:
            while True:
                message = await self.connection.recv()
                if message is None:
                    logging.info("Connection closed by the server.")
                    await self.close()
                    break
                request_id, success, result = message
                sink = self.pending_requests.get(request_id)
                if sink:
                    try:
                        sink.send_nowait((success, result))
                    except:
                        traceback.print_exc()
                else:
                    (f"Failed to find {request_id}")
        except websockets.exceptions.ConnectionClosedOK:
            pass

    async def send_command(self, command, args, process_value=None):
        request_id = next(self.request_id)
        sink, stream = anyio.create_memory_object_stream()
        try:
            self.pending_requests[request_id] = sink
            await self.send(request_id, command, args)
            success, result = await stream.receive()
            if not success:
                raise Exception(result)
            return process_value(result) if process_value else result
        finally:
            self.pending_requests.pop(request_id)

    async def subscribe_stream(self, return_sink, command, args, process_value):
        request_id = next(self.request_id)
        request_sink, request_stream = anyio.create_memory_object_stream(100)
        try:
            self.pending_requests[request_id] = request_sink
            await self.send(request_id, command, args)
            while True:
                success, values = await request_stream.receive()
                if not success:
                    raise Exception(values)
                if values is None:
                    break
                for value in values:
                    await return_sink(process_value(*value))
        finally:
            await self.send(request_id, None, None)
            self.pending_requests.pop(request_id)


class AsyncClient(AsyncClientBase):
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

        return create_context_async_generator(
            self.subscribe_stream,
            "read_events",
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
        return await self.send_command(
            "write_event",
            (topic, self.serializer(event), time_stamp, override),
        )

    async def read_event(self, topic: str, time_stamp: datetime):
        return await self.send_command(
            "read_event", (topic, time_stamp), self.deserializer
        )


@contextlib.asynccontextmanager
async def _create_async_client(
    task_group, connection: Connection, name: str
) -> AsyncIterable[AsyncClient]:
    client = AsyncClient(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client


@contextlib.asynccontextmanager
async def create_async_client(
    host_name: str, port: str, name: str
) -> AsyncIterable[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with wrap_websocket_connection(host_name, port) as connection:
            async with _create_async_client(task_group, connection, name) as client:
                yield client
