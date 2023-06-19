from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from client_sync import SyncClient

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
from .common import UserException, cancel_task_on_exit, scoped_insert, scoped_iter
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
SET_ATTRIBUTE = "Set attribute"
ASYNC_ITERATOR = "Async iterator"
AWAITABLE = "Awaitable"
FUNCTION = "Function"
ITER_ASYNC_ITERATOR = "Iter async iterator"
STREAM_BUFFER_SIZE = 10

SERVER_OBJECT_ID = 0

ASYNC_SETATTR_ERROR_MESSAGE = "Cannot set attribute on remote object in async mode. Use setattr method instead. \
We intentionally do not support setting attributes using assignment operator on remote objects in async mode. \
This is because it is not a good practice not too wait until a remote operation completes."

ASYNC_GENERATOR_NOT_OVERFLOWED = False
ASYNC_GENERATOR_OVERFLOWED_MESSAGE = "Async generator overflowed."


class Result(AsyncIterable):
    def __init__(self, client, object_id: int, function: Callable, args, kwargs):
        self.client: "AsyncClient" = client
        self.object_id = object_id
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return asyncio.create_task(
            self.client.execute_request(
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


class Value:
    """Wrapper for a value that would be passed by copy preventing it from being accessed to by parts of the code."""

    def __init__(self, content):
        self.content = content

    def __call__(self) -> Any:
        return self.content

    def set(self, value):
        self.content = value


class AsyncClient:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.async_generators_streams = {}
        self.remote_objects = {}

    async def send(self, *args):
        await self.connection.send(args)

    def send_nowait(self, *args):
        self.connection.send_nowait(args)

    async def process_messages_from_server(
        self, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        try:
            async for code, message_id, status, result in self.connection:
                if code in (
                    FUNCTION,
                    AWAITABLE,
                    GET_ATTRIBUTE,
                    CREATE_OBJECT,
                    FETCH_OBJECT,
                    SET_ATTRIBUTE,
                ):
                    future = self.pending_requests.get(message_id)
                    if future:
                        future.set_result((status, result))
                elif code == ITER_ASYNC_ITERATOR:
                    if sink_and_overflow_flag := self.async_generators_streams.get(message_id):
                        sink, overflow_flag = sink_and_overflow_flag
                        try:
                            sink.send_nowait((status, result))
                        except anyio.WouldBlock:
                            overflow_flag.set(True)
                            sink.close()
                elif code == CANCELLED_TASK:
                    print("Task cancelled.", message_id, status, result)
                else:
                    print(f"Unexpected code {code} received.")
        except Exception as e:
            print(e)

    @contextlib.contextmanager
    def submit_request(self, code, args, is_cancellable=False):
        request_id = next(self.request_id)
        future = asyncio.Future()
        with scoped_insert(self.pending_requests, request_id, future):
            self.send_nowait(code, request_id, args)
            try:
                yield future
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    self.cancel_request_no_wait(request_id)
                raise

    async def execute_request(self, code, args, is_cancellable=False, include_code=True) -> Any:
        with self.submit_request(code, args, is_cancellable) as future:
            return decode_result(*(await future), include_code=include_code)

    async def get_attribute(self, object_id: int, name: str):
        return await self.execute_request(GET_ATTRIBUTE, (object_id, name), include_code=False)

    async def set_attribute(self, object_id: int, name: str, value: Any):
        return await self.execute_request(
            SET_ATTRIBUTE, (object_id, name, value), include_code=False
        )

    def remote_method(self, object_id: int, function: Callable, *args, **kwargs) -> Any:
        return Result(self, object_id, function, args, kwargs)

    def cancel_request_no_wait(self, request_id: int):
        self.send_nowait(CANCELLED_TASK, request_id, None)
        self.pending_requests.pop(request_id, None)

    async def cancel_request(self, request_id: int):
        self.send_nowait(CANCELLED_TASK, request_id, None)
        self.pending_requests.pop(request_id, None)
        await self.connection.drain()

    async def evaluate_async_generator(self, object_id, function, args, kwargs):
        iterator_id = await self.execute_request(
            FUNCTION,
            (object_id, function, args, kwargs),
            is_cancellable=True,
            include_code=False,
        )
        async for value in self.iter_async_generator(iterator_id):
            yield value

    async def iter_async_generator(self, iterator_id: int):
        sink, stream = anyio.create_memory_object_stream(STREAM_BUFFER_SIZE)
        request_id = next(self.request_id)
        over_flow_flag = Value(ASYNC_GENERATOR_NOT_OVERFLOWED)
        with scoped_insert(self.async_generators_streams, request_id, (sink, over_flow_flag)):
            await self.send(ITER_ASYNC_ITERATOR, request_id, iterator_id)
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
                await self.cancel_request(request_id)
                if over_flow_flag():
                    raise OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional[SyncClient] = None
    ):
        object_id = await self.execute_request(
            CREATE_OBJECT, (object_class, args, kwarg), include_code=False
        )
        try:
            yield await self.fetch_remote_object(object_id, sync_client)
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(DELETE_OBJECT, 0, object_id)

    async def fetch_remote_object(
        self, object_id: int = SERVER_OBJECT_ID, sync_client: Optional[SyncClient] = None
    ) -> Any:
        if object_id not in self.remote_objects:
            object_class = await self.execute_request(FETCH_OBJECT, object_id, include_code=False)
            setattr = partial(self.set_attribute, object_id)
            __getattr__ = partial(self.get_attribute, object_id)
            if sync_client:
                __getattr__ = sync_client.wrap_awaitable(__getattr__)
                setattr = __setattr__ = sync_client.wrap_awaitable(setattr)
            else:

                def __setattr__(_self, _name, _value):
                    raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)

            object_class = type(
                f"{object_class.__name__}Proxy",
                (RemoteObject, object_class),
                {"__getattr__": __getattr__, "setattr": setattr, "__setattr__": __setattr__},
            )
            remote_object = object_class.__new__(object_class)
            object.__setattr__(remote_object, "client", self)
            object.__setattr__(remote_object, "object_id", object_id)
            for name in dir(object_class):
                if name.startswith("__") and name.endswith("__"):
                    continue
                attribute = getattr(object_class, name)
                if inspect.isfunction(attribute):
                    method = partial(self.remote_method, object_id, attribute)
                    if sync_client:
                        method = sync_client.wrap_function(object_id, attribute)
                    object.__setattr__(remote_object, name, method)
            self.remote_objects[object_id] = remote_object
        return self.remote_objects[object_id]


@contextlib.asynccontextmanager
async def _create_async_client(connection: Connection) -> AsyncIterator[AsyncClient]:
    client = AsyncClient(connection)
    with cancel_task_on_exit(client.process_messages_from_server()):
        yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with connect_to_tcp_server(host_name, port) as connection:
            async with _create_async_client(connection) as client:
                yield client
