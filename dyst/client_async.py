import asyncio
import contextlib
import inspect
import sys
from functools import partial
from itertools import count
from typing import Any, AsyncIterator

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
COMMAND = "Command"
CREATE_OBJECT = "Create object"
GET_ATTRIBUTE = "Get attribute"
STREAM_BUFFER = 100
RESULT = "Result"
IS_GENERATOR = True
IS_COROUTINE = False


def remote_iter(method):
    method.__name__ += "@remote_iter"
    return method


def remote(method):
    method.__name__ += "@remote"
    return method


async def fetch_attribute(self, name):
    request_id = next(self.client.request_id)
    sink, stream = anyio.create_memory_object_stream()
    self.client.pending_requests[request_id] = sink
    await self.client.send(GET_ATTRIBUTE, (self.object_id, request_id, name))
    code, value = await stream.receive()
    return value


class AsyncClientCore:
    """Implements non functional specific details."""

    def __init__(self, task_group, connection: Connection, name=None) -> None:
        self.task_group = task_group
        self.connection = connection
        self.request_id = count()
        self.pending_requests = {}
        self.name = name
        self.object_counter = count()
        self.pending_objects = {}

    async def send(self, *args):
        await self.connection.send(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        await self.send(self.name)
        task_status.started()
        async for code, message in self.connection:
            if code == RESULT:
                request_id, code, result = message
                sink = self.pending_requests.get(request_id)
                if sink:
                    sink.send_nowait((code, result))
            elif code == CREATE_OBJECT:
                object_id, code, message = message
                self.pending_objects.pop(object_id).set_result((code, message))

    @contextlib.asynccontextmanager
    async def create_remote_object(self, object_class, args, kwarg):
        object_id = next(self.object_counter)
        self.pending_objects[object_id] = asyncio.Future()
        try:
            await self.send(CREATE_OBJECT, (object_id, object_class, args, kwarg))
            code, details = await self.pending_objects[object_id]
            if code != OK:
                raise Exception(details)
            object_class = type(
                object_class.__name__, (object_class,), {"__getattr__": fetch_attribute}
            )
            remote_object = object_class.__new__(object_class)
            remote_object.client = self
            remote_object.object_id = object_id
            for name, attr in ((name, getattr(object_class, name)) for name in dir(object_class)):
                if inspect.isfunction(attr):
                    if attr.__name__.endswith("@remote"):
                        setattr(
                            remote_object, name, partial(self.evaluate_command, object_id, name)
                        )
                    elif attr.__name__.endswith("@remote_iter"):
                        setattr(
                            remote_object,
                            name,
                            partial(self.create_cancellable_stream, object_id, name),
                        )
            yield remote_object
        finally:
            await self.send(CREATE_OBJECT, (object_id, None, None, None))

    @contextlib.asynccontextmanager
    async def manage_request(self, object_id, command, is_generator, args, buffer_size=1):
        sink, stream = anyio.create_memory_object_stream(buffer_size)
        request_id = next(self.request_id)
        self.pending_requests[request_id] = sink
        await self.send(COMMAND, (object_id, request_id, command, is_generator, args))
        try:
            yield stream
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(COMMAND, (object_id, request_id, None, None, None))
            self.pending_requests.pop(request_id, None)

    async def evaluate_command(self, object_id, command: Any, *args, **kwargs):
        async with self.manage_request(object_id, command, IS_COROUTINE, args) as stream:
            code, result = await stream.receive()
            if code == OK:
                return result
            elif code == CANCELLED_TASK:
                return
            if code == USER_EXCEPTION:
                raise UserException(result)
            raise Exception(result)

    async def create_cancellable_stream(self, object_id, command, *args, **kwargs):
        request = self.manage_request(object_id, command, IS_GENERATOR, args, STREAM_BUFFER)
        try:
            stream = await request.__aenter__()
            async for code, value in stream:
                if code == OK:
                    yield value
                elif code in (CLOSE_SENTINEL, CANCELLED_TASK):
                    break
                else:
                    raise Exception(value)
        finally:
            await request.__aexit__(None, None, None)


@contextlib.asynccontextmanager
async def _create_async_client_core(
    task_group, connection: Connection, name: str
) -> AsyncIterator[AsyncClientCore]:
    client = AsyncClientCore(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client
