from __future__ import annotations

import asyncio
import contextlib
import inspect
import queue
from collections.abc import AsyncIterable
from functools import partial
from itertools import count
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Dict, Optional

import anyio
import anyio.abc
import asyncstdlib

from .abc import AsyncSink, Connection
from .common import UserException, cancel_task_on_exit, scoped_insert
from .connection import connect_to_tcp_server


if TYPE_CHECKING:
    from .client_sync import SyncClient


OK = "OK"
CLOSE_SENTINEL = "Close sentinel"
CANCELLED_TASK = "Cancelled task"
EXCEPTION = "Exception"
USER_EXCEPTION = "UserException"
CREATE_OBJECT = "Create object"
DELETE_OBJECT = "Delete object"
FETCH_OBJECT = "Fetch object"
GET_ATTRIBUTE = "Get attribute"
SET_ATTRIBUTE = "Set attribute"
ASYNC_ITERATOR = "Async iterator"
MOVE_ASYNC_ITERATOR = "Move Async iterator"
METHOD = "Function"
ITER_ASYNC_ITERATOR = "Iter async iterator"
STREAM_BUFFER_SIZE = 10

SERVER_OBJECT_ID = 0

ASYNC_SETATTR_ERROR_MESSAGE = "Cannot set attribute on remote object in async mode. Use setattr method instead. \
We intentionally do not support setting attributes using assignment operator on remote objects in async mode. \
This is because it is not a good practice not too wait until a remote operation completes."

ASYNC_GENERATOR_OVERFLOWED_MESSAGE = "Async generator overflowed."

REQUEST_CODES = (METHOD, GET_ATTRIBUTE, CREATE_OBJECT, FETCH_OBJECT, SET_ATTRIBUTE)


class IterationBufferSync(AsyncSink):
    def __init__(self, size: int) -> None:
        self._overflowed = False
        self._size = size
        self._queue = queue.SimpleQueue()

    def send_nowait(self, value: Any):
        self._queue.put_nowait(value)
        if self._queue.qsize() >= self._size:
            self._overflowed = True
            raise asyncio.QueueFull()

    def __iter__(self):
        return self

    def __next__(self):
        if self._overflowed:
            raise OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)
        return self._queue.get()


class IterationBufferAsync(AsyncSink):
    def __init__(self, size: int) -> None:
        self._overflowed = False
        self._queue = asyncio.Queue(maxsize=size)

    def send_nowait(self, value: Any):
        try:
            self._queue.put_nowait(value)
        except asyncio.QueueFull:
            self._overflowed = True
            raise

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._overflowed:
            raise OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)
        return await self._queue.get()


class AsyncCallResult(AsyncIterable):
    def __init__(self, client, object_id: int, function: Callable, args, kwargs):
        self.client: AsyncClient = client
        self.object_id = object_id
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return asyncio.create_task(
            self.client.execute_request(
                METHOD,
                (self.object_id, self.function, self.args, self.kwargs),
                is_cancellable=True,
                include_code=False,
            )
        ).__await__()

    def __aiter__(self):
        return self.client._evaluate_async_generator(
            self.object_id, self.function, self.args, self.kwargs
        )


def decode_result(code, result, include_code=True):
    if code in (CANCELLED_TASK, OK, ASYNC_ITERATOR):
        return (code, result) if include_code else result
    if code == USER_EXCEPTION:
        raise UserException(result)
    if code == EXCEPTION:
        if isinstance(result, Exception):
            raise result
        raise Exception(result)
    else:
        raise Exception(f"Unexpected code {code} received.")


def decode_iteration_result(code, result):
    if code in (CLOSE_SENTINEL, CANCELLED_TASK):
        return True, None
    return False, decode_result(code, result, include_code=False)


class RemoteObject:
    def __init__(self, client, object_id: int):
        self.client = client
        self.object_id = object_id


def __setattr_forbidden__(_self, _name, _value):
    raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)


class AsyncClient:
    def __init__(
        self, connection: Connection, async_buffer_size: int = STREAM_BUFFER_SIZE
    ) -> None:
        self.connection = connection
        self._async_buffer_size = async_buffer_size
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self._iteraton_buffers: Dict[int, AsyncSink] = {}
        self.remote_objects = {}

    async def _send(self, *args):
        await self.connection.send(args)

    def _send_nowait(self, *args):
        self.connection.send_nowait(args)

    async def _process_messages_from_server(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for code, message_id, status, result in self.connection:
            if code in REQUEST_CODES:
                if future := self.pending_requests.get(message_id):
                    future.set_result((status, result))
            elif code == ITER_ASYNC_ITERATOR:
                if queue := self._iteraton_buffers.get(message_id):
                    queue.send_nowait((status, result))
            elif code == CANCELLED_TASK:
                print("Task cancelled.", message_id, status, result)
            else:
                print(f"Unexpected code {code} received.")

    @contextlib.contextmanager
    def _submit_request(self, code, args, is_cancellable=False):
        request_id = next(self.request_id)
        future = asyncio.Future()
        with scoped_insert(self.pending_requests, request_id, future):
            self._send_nowait(code, request_id, args)
            try:
                yield future
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    self._cancel_request_no_wait(request_id)
                raise

    async def execute_request(self, code, args, is_cancellable=False, include_code=True) -> Any:
        with self._submit_request(code, args, is_cancellable) as future:
            return decode_result(*(await future), include_code=include_code)

    async def _get_attribute(self, object_id: int, name: str):
        return await self.execute_request(GET_ATTRIBUTE, (object_id, name), include_code=False)

    async def _set_attribute(self, object_id: int, name: str, value: Any):
        return await self.execute_request(
            SET_ATTRIBUTE, (object_id, name, value), include_code=False
        )

    def _call_method_remotely(self, object_id: int, function: Callable, *args, **kwargs) -> Any:
        return AsyncCallResult(self, object_id, function, args, kwargs)

    def _cancel_request_no_wait(self, request_id: int):
        self._send_nowait(CANCELLED_TASK, request_id, None)
        self.pending_requests.pop(request_id, None)

    async def _cancel_request(self, request_id: int):
        self._cancel_request_no_wait(request_id)
        await self.connection.drain()

    async def _evaluate_async_generator(self, object_id, function, args, kwargs):
        push_or_pull, generator_id = await self.execute_request(
            METHOD,
            (object_id, function, args, kwargs),
            is_cancellable=True,
            include_code=False,
        )
        queue = IterationBufferAsync(self._async_buffer_size)
        request_id = next(self.request_id)
        with scoped_insert(self._iteraton_buffers, request_id, queue):
            await self._send(ITER_ASYNC_ITERATOR, request_id, (generator_id,))
            try:
                async for index, (terminated, value) in asyncstdlib.enumerate(
                    asyncstdlib.starmap(decode_iteration_result, queue)
                ):
                    if terminated:
                        break
                    yield value
                    if not push_or_pull:
                        await self._send(MOVE_ASYNC_ITERATOR, generator_id, (index + 1,))
            finally:
                await self._cancel_request(request_id)

    @contextlib.asynccontextmanager
    async def _remote_sync_generator_iter(self, iterator_id: int):
        queue = IterationBufferSync(self._async_buffer_size)
        request_id = next(self.request_id)
        with scoped_insert(self._iteraton_buffers, request_id, queue):
            try:
                await self._send(ITER_ASYNC_ITERATOR, request_id, (iterator_id,))
                yield queue
            finally:
                await self._cancel_request(request_id)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional[SyncClient] = None
    ):
        object_id = await self.execute_request(
            CREATE_OBJECT, (object_class, args, kwarg), include_code=False
        )
        try:
            yield await self._fetch_remote_object(object_id, sync_client)
        finally:
            with anyio.CancelScope(shield=True):
                await self._send(DELETE_OBJECT, 0, object_id)

    async def _fetch_remote_object(
        self, object_id: int = SERVER_OBJECT_ID, sync_client: Optional[SyncClient] = None
    ) -> Any:
        if object_id not in self.remote_objects:
            object_class = await self.execute_request(FETCH_OBJECT, object_id, include_code=False)
            setattr = partial(self._set_attribute, object_id)
            __getattr__ = partial(self._get_attribute, object_id)
            if sync_client:
                __getattr__ = sync_client._wrap_awaitable(__getattr__)
                setattr = __setattr__ = sync_client._wrap_awaitable(setattr)
            else:
                __setattr__ = __setattr_forbidden__
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
                    method = partial(self._call_method_remotely, object_id, attribute)
                    if sync_client:
                        method = sync_client._wrap_function(object_id, attribute)
                    object.__setattr__(remote_object, name, method)
            self.remote_objects[object_id] = remote_object
        return self.remote_objects[object_id]

    async def fetch_remote_object(self, object_id: int = SERVER_OBJECT_ID):
        return await self._fetch_remote_object(object_id)


@contextlib.asynccontextmanager
async def create_async_client(connection: Connection) -> AsyncIterator[AsyncClient]:
    client = AsyncClient(connection)
    with cancel_task_on_exit(client._process_messages_from_server()):
        yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with connect_to_tcp_server(host_name, port) as connection:
            async with create_async_client(connection) as client:
                yield client
