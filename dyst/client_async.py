import contextlib
import logging
from itertools import count
from typing import Any, AsyncIterable, Optional, Callable, Tuple

import anyio
import websockets
from anyio.abc import TaskStatus

from .common import create_context_async_generator, identity
from .connection import Connection


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

    def create_cancellable_stream(
        self, command, args: Tuple[Any, ...], process_value: Optional[Callable]
    ):
        process_value = process_value or identity

        async def cancellable_stream(sink: Callable):
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
                    await sink(process_value(value))
            finally:
                with anyio.CancelScope(shield=True):
                    await self.send(request_id, None, None)
                    self.pending_requests.pop(request_id)

        return cancellable_stream

    def subscribe_stream(self, command, args, process_value=None):
        return create_context_async_generator(
            self.create_cancellable_stream(command, args, process_value)
        )


@contextlib.asynccontextmanager
async def _create_async_client_core(
    task_group, connection: Connection, name: str
) -> AsyncIterable[AsyncClientCore]:
    client = AsyncClientCore(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client
