import asyncio
import contextlib
import inspect
from functools import partial
from itertools import count
from typing import Any, AsyncIterator, Optional

import anyio
from anyio.abc import TaskStatus

from .abc import Connection
from .common import UserException, scoped_insert
from .connection import connect_to_tcp_server


OK = "OK"
CLOSE_SENTINEL = "Close sentinel"
CLOSE_STREAM = "Close stream"
CANCELLED_TASK = "Cancelled task"
EXCEPTION = "Exception"
USER_EXCEPTION = "UserException"
CREATE_OBJECT = "Create object"
GET_ATTRIBUTE = "Get attribute"
ASYNC_GENERATOR = "Async generator"
COROUTINE = "Coroutine"
STREAM_BUFFER = 100


def remote_iter(method):
    method.__name__ += "@remote_iter"
    return method


def remote(method):
    method.__name__ += "@remote"
    return method


class AsyncClient:
    """Implements non functional specific details."""

    def __init__(self, task_group, connection: Connection, name=None) -> None:
        self.task_group = task_group
        self.connection = connection
        self.name = name
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.current_streams = {}

    async def send(self, *args):
        await self.connection.send(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        await self.send(self.name)
        task_status.started()
        async for code, message_id, (status, result) in self.connection:
            if code in (COROUTINE, GET_ATTRIBUTE, CREATE_OBJECT):
                future = self.pending_requests.get(message_id)
                if future:
                    future.set_result((status, result))
            elif code == ASYNC_GENERATOR:
                sink = self.current_streams.get(message_id)
                if sink:
                    sink.send_nowait((status, result))
            else:
                raise Exception(f"Unexpected code {code} received.")

    async def manage_request(self, code, args, is_cancellable=False) -> Any:
        request_id = next(self.request_id)
        future = asyncio.Future()
        with scoped_insert(self.pending_requests, request_id, future):
            try:
                await self.send(code, request_id, args)
                code, result = await future
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    with anyio.CancelScope(shield=True):
                        await self.send(code, request_id, None)
                raise
        if code == OK:
            return result
        elif code == CANCELLED_TASK:
            return
        if code == USER_EXCEPTION:
            raise UserException(result)
        else:
            if isinstance(result, Exception):
                raise result
            raise Exception(result)

    def get_attribute(self, object_id: int, name: str):
        return self.manage_request(GET_ATTRIBUTE, (object_id, name))

    async def evaluate_coroutine(self, object_id: int, command: str, *args, **kwargs) -> Any:
        return await self.manage_request(
            COROUTINE, (object_id, command, args, kwargs), is_cancellable=True
        )

    async def evaluate_async_generator(self, object_id, command, *args, **kwargs):
        sink, stream = anyio.create_memory_object_stream(STREAM_BUFFER)
        stream_id = next(self.request_id)
        with scoped_insert(self.current_streams, stream_id, sink):
            await self.send(ASYNC_GENERATOR, stream_id, (object_id, command, args, kwargs))
            try:
                async for code, value in stream:
                    if code == OK:
                        yield value
                    elif code in (CLOSE_SENTINEL, CANCELLED_TASK):
                        break
                    elif code == USER_EXCEPTION:
                        raise UserException(value)
                    else:
                        raise Exception(value)
            finally:
                with anyio.CancelScope(shield=True):
                    await self.send(ASYNC_GENERATOR, stream_id, None)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional["SyncClient"] = None
    ):
        object_id = next(self.object_id)
        await self.manage_request(CREATE_OBJECT, (object_id, object_class, args, kwarg))
        try:
            __getattr__ = partial(self.get_attribute, object_id)
            if sync_client:
                __getattr__ = partial(sync_client.wrap_awaitable(__getattr__))
            object_class = type(
                f"{object_class.__name__}Proxy",
                (object_class,),
                {"__getattr__": __getattr__},
            )
            remote_object = object_class.__new__(object_class)
            remote_object.client = self
            remote_object.object_id = object_id
            for name, attr in ((name, getattr(object_class, name)) for name in dir(object_class)):
                if inspect.isfunction(attr):
                    if attr.__name__.endswith("@remote"):
                        method = partial(self.evaluate_coroutine, object_id, name)
                        if sync_client:
                            method = partial(sync_client.wrap_awaitable(method))
                        setattr(remote_object, name, method)
                    elif attr.__name__.endswith("@remote_iter"):
                        method = partial(self.evaluate_async_generator, object_id, name)
                        if sync_client:
                            method = partial(sync_client.wrap_async_context_stream(method))
                        setattr(remote_object, name, method)
            yield remote_object
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(CREATE_OBJECT, 0, (object_id, None, None, None))


@contextlib.asynccontextmanager
async def _create_async_client_core(
    task_group, connection: Connection, name: str
) -> AsyncIterator[AsyncClient]:
    client = AsyncClient(task_group, connection, name=name)
    await task_group.start(client.process_messages_from_server)
    yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int, name: str) -> AsyncIterator[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with connect_to_tcp_server(host_name, port) as connection:
            async with _create_async_client_core(task_group, connection, name) as client:
                yield client
