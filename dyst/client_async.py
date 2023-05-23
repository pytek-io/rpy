import contextlib
import logging
import sys
from itertools import count
from typing import Any, AsyncIterator, Callable, Optional, Tuple

import anyio
from anyio.abc import TaskStatus

from .abc import Connection
from .common import create_context_async_generator, identity
from .exception import UserException

if sys.version_info < (3, 10):
    from asyncstdlib import anext

OK = "OK"
START_TASK = "Start task"
CLOSE_SENTINEL = "Done"
SHUTDOWN = "Shutdown"
CLOSE_STREAM = "Close stream"
CANCEL_TASK = "Cancel task"
CANCELLED_TASK = "Cancelled task"
EXCEPTION = "Exception"
USER_EXCEPTION = "UserException"

STREAM_BUFFER = 100


class AsyncClientCore:
    """Implements non functional specific details."""

    def __init__(self, task_group, connection: Connection, name=None) -> None:
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
        while True:
            message = await anext(self.connection)
            if message is None:
                logging.info("Connection closed by the server.")
                break
            request_id, code, result = message
            sink = self.pending_requests.get(request_id)
            if sink:
                sink.send_nowait((code, result))

    @contextlib.asynccontextmanager
    async def manage_request(self, command, args, buffer_size=1):
        sink, stream = anyio.create_memory_object_stream(buffer_size)
        request_id = next(self.request_id)
        self.pending_requests[request_id] = sink
        await self.send(request_id, command.__name__, args)
        try:
            yield stream
        except anyio.get_cancelled_exc_class():
            with anyio.CancelScope(shield=True):
                await self.send(request_id, None, None)
            raise
        finally:
            self.pending_requests.pop(request_id)

    async def evaluate_command(self, command: Any, args: Tuple[Any, ...]):
        async with self.manage_request(command, args) as stream:
            code, result = await stream.receive()
            if code == OK:
                return result
            elif code == CANCELLED_TASK:
                return
            if code == USER_EXCEPTION:
                raise UserException(result)
            raise Exception(result)

    def create_cancellable_stream(
        self, command, args: Tuple[Any, ...], process_value: Optional[Callable]
    ):
        process_value = process_value or identity

        async def cancellable_stream(sink: Callable):
            async with self.manage_request(command, args, STREAM_BUFFER) as stream:
                async for code, value in stream:
                    if code == OK:
                        await sink(process_value(value))
                    elif code in (CLOSE_SENTINEL, CANCELLED_TASK):
                        break
                    else:
                        raise Exception(value)

        return cancellable_stream

    def subscribe_stream(self, command, args, process_value=None):
        return create_context_async_generator(
            self.create_cancellable_stream(command, args, process_value)
        )


@contextlib.asynccontextmanager
async def _create_async_client_core(
    task_group, connection: Connection, name: str
) -> AsyncIterator[AsyncClientCore]:
    client = AsyncClientCore(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client
