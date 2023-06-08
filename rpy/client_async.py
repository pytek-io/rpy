import asyncio
import contextlib
import inspect
from collections.abc import AsyncIterable
from functools import partial
from itertools import count
from typing import Any, AsyncIterator, Callable, Optional

import anyio
from anyio.abc import TaskStatus

from .abc import Connection
from .common import UserException, scoped_insert, scoped_execute_coroutine, closing_scope
from .connection import connect_to_tcp_server


OK = "OK"
CLOSE_SENTINEL = "Close sentinel"
CLOSE_STREAM = "Close stream"
CANCELLED_TASK = "Cancelled task"
EXCEPTION = "Exception"
USER_EXCEPTION = "UserException"
CREATE_OBJECT = "Create object"
DELETE_OBJECT = "Delete object"
FETCH_OBJECT = "Fetch object"
GET_ATTRIBUTE = "Get attribute"
ASYNC_ITERATOR = "Async iterator"
AWAITABLE = "Awaitable"
FUNCTION = "Function"
ITER_ASYNC_ITERATOR = "Iter async iterator"
STREAM_BUFFER = 100

SERVER_OBJECT_ID = 0


class Result(AsyncIterable):
    def __init__(self, client, object_id: int, function: Callable, args, kwargs):
        self.client: "AsyncClient" = client
        self.object_id = object_id
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return asyncio.create_task(
            self.client.manage_request(
                AWAITABLE,
                (self.object_id, self.function, self.args, self.kwargs),
                is_cancellable=True,
                include_code=False,
            )
        ).__await__()

    def __aiter__(self):
        return self.client.evaluate_async_generator(
            self.object_id, self.function, self.args, self.kwargs
        )


def decode_result(code, result, include_code=True):
    if code in (CANCELLED_TASK, OK, AWAITABLE, ASYNC_ITERATOR):
        return (code, result) if include_code else result
    if code == USER_EXCEPTION:
        raise UserException(result)
    if code == EXCEPTION:
        if isinstance(result, Exception):
            raise result
        raise Exception(result)
    else:
        raise Exception(f"Unexpected code {code} received.")


class RemoteObject:
    def __init__(self, client, object_id: int):
        self.client = client
        self.object_id = object_id


class AsyncClient:
    def __init__(self, task_group, connection: Connection) -> None:
        self.task_group = task_group
        self.connection = connection
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.current_streams = {}
        self.remote_objects = {}
        self.closed = False

    def close(self):
        self.closed = True

    async def send(self, *args):
        if self.closed:
            raise RuntimeError("Client closed.")
        await self.connection.send(args)

    def send_nowait(self, *args):
        if self.closed:
            raise RuntimeError("Client closed.")
        self.connection.send_nowait(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for code, message_id, status, result in self.connection:
            if code in (FUNCTION, AWAITABLE, GET_ATTRIBUTE, CREATE_OBJECT, FETCH_OBJECT):
                future = self.pending_requests.get(message_id)
                if future:
                    future.set_result((status, result))
            elif code in (ASYNC_ITERATOR, ITER_ASYNC_ITERATOR):
                sink = self.current_streams.get(message_id)
                if sink:
                    sink.send_nowait((status, result))
            else:
                print(f"Unexpected code {code} received.")

    @contextlib.contextmanager
    def create_request(self, code, args, is_cancellable=False):
        request_id = next(self.request_id)
        future = asyncio.Future()
        with scoped_insert(self.pending_requests, request_id, future):
            self.send_nowait(code, request_id, args)
            try:
                yield future
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    with anyio.CancelScope(shield=True):
                        self.send_nowait(code, request_id, None)
                raise

    async def manage_request(self, code, args, is_cancellable=False, include_code=True) -> Any:
        with self.create_request(code, args, is_cancellable) as future:
            return decode_result(*(await future), include_code=include_code)

    async def get_attribute(self, object_id: int, name: str):
        return await self.manage_request(GET_ATTRIBUTE, (object_id, name), include_code=False)

    def remote_method(self, object_id: int, function: Callable, *args, **kwargs) -> Any:
        return Result(self, object_id, function, args, kwargs)

    async def evaluate_async_generator(self, object_id, function, args, kwargs):
        sink, stream = anyio.create_memory_object_stream(STREAM_BUFFER)
        stream_id = next(self.request_id)
        with scoped_insert(self.current_streams, stream_id, sink):
            await self.send(ASYNC_ITERATOR, stream_id, (object_id, function, args, kwargs))
            try:
                async for code, value in stream:
                    if code in (ASYNC_ITERATOR, OK):
                        yield value
                    elif code in (CLOSE_SENTINEL, CANCELLED_TASK):
                        break
                    elif code == USER_EXCEPTION:
                        raise UserException(value)
                    else:
                        raise Exception(value)
            finally:
                with anyio.CancelScope(shield=True):
                    await self.send(ASYNC_ITERATOR, stream_id, None)

    async def iter_async_generator(self, iterator_id: int):
        sink, stream = anyio.create_memory_object_stream(STREAM_BUFFER)
        stream_id = next(self.request_id)
        with scoped_insert(self.current_streams, stream_id, sink):
            await self.send(ITER_ASYNC_ITERATOR, stream_id, iterator_id)
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
                    await self.send(ASYNC_ITERATOR, stream_id, None)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional["SyncClient"] = None
    ):
        object_id = await self.manage_request(
            CREATE_OBJECT, (object_class, args, kwarg), include_code=False
        )
        try:
            yield await self.fetch_remote_object(object_id, sync_client)
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(DELETE_OBJECT, 0, object_id)

    async def fetch_remote_object(
        self, object_id: int = SERVER_OBJECT_ID, sync_client: Optional["SyncClient"] = None
    ) -> Any:
        if object_id not in self.remote_objects:
            object_class = await self.manage_request(FETCH_OBJECT, object_id, include_code=False)
            __getattr__ = partial(self.get_attribute, object_id)
            if sync_client:
                __getattr__ = sync_client.wrap_awaitable(__getattr__)
            object_class = type(
                f"{object_class.__name__}Proxy",
                (RemoteObject, object_class),
                {"__getattr__": __getattr__},
            )
            remote_object = object_class.__new__(object_class)
            RemoteObject.__init__(remote_object, self, object_id)
            for name in dir(object_class):
                if name.startswith("__") and name.endswith("__"):
                    continue
                attribute = getattr(object_class, name)
                if inspect.isfunction(attribute):
                    method = partial(self.remote_method, object_id, attribute)
                    if sync_client:
                        method = sync_client.wrap_function(object_id, attribute)
                    setattr(remote_object, name, method)
            self.remote_objects[object_id] = remote_object
        return self.remote_objects[object_id]


@contextlib.asynccontextmanager
async def _create_async_client(task_group, connection: Connection) -> AsyncIterator[AsyncClient]:
    with closing_scope(AsyncClient(task_group, connection)) as client:
        with scoped_execute_coroutine(client.process_messages_from_server()):
            yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with connect_to_tcp_server(host_name, port) as connection:
            async with _create_async_client(task_group, connection) as client:
                yield client
