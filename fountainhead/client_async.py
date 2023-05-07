import contextlib
import datetime
import logging
import traceback
from itertools import count
from typing import Any, Optional

import anyio
import websockets
from msgpack import dumps, loads  # could be any arbitrary serialization method

from .common import create_context_async_generator
from .connection import Connection

OK = "OK"
START_TASK = "Start task"
CLOSE_SENTINEL = "Done"
SHUTDOWN = "Shutdown"
CLOSE_STREAM = "Close stream"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"


class Client:
    def __init__(
        self, task_group, connection, serializer=None, deserializer=None
    ) -> None:
        super().__init__()
        self.task_group = task_group
        self.connection = connection
        self.serializer = serializer or dumps
        self.deserializer = deserializer or loads
        self.request_id = count()
        self.pending_requests = {}

    async def close(self):
        logging.info("Closing connection.")
        await self.connection.close()
        self.task_group.cancel_scope.cancel()

    async def send(self, *args):
        await self.connection.send(args)

    async def process_messages_from_server(self):
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

    async def send_command(self, command, *args):
        request_id = next(self.request_id)
        sink, stream = anyio.create_memory_object_stream()
        try:
            self.pending_requests[request_id] = sink
            await self.send(request_id, command, args)
            success, result = await stream.receive()
            if not success:
                raise Exception(result)
            return result
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

    def read_events(
        self,
        topic: str,
        start: datetime.datetime,
        end: Optional[datetime.datetime],
        time_stamps_only=False,
    ):
        if time_stamps_only:
            process_value = datetime.datetime.fromtimestamp
        else:
            def process_value(time_stamp, value):
                return datetime.datetime.fromtimestamp(time_stamp), loads(value)
            
        return create_context_async_generator(
            self.subscribe_stream, "read_events", (topic, start, end, time_stamps_only), process_value
        )

    async def save_event(self, topic: str, event: Any):
        return await self.send_command("save_event", topic, self.serializer(event))


@contextlib.asynccontextmanager
async def _create_async_client(task_group, host_name: str, port: str) -> Client:
    async with websockets.connect(f"ws://{host_name}:{port}") as raw_websocket:
        client = Client(task_group, Connection(raw_websocket))
        task_group.start_soon(client.process_messages_from_server)
        yield client

@contextlib.asynccontextmanager
async def create_async_client(host_name: str, port: str) -> Client:
    async with anyio.create_task_group() as task_group:
        async with _create_async_client(task_group, host_name, port) as client:
            yield client