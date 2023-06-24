from __future__ import annotations

import asyncio
import contextlib
import traceback
from itertools import count
from typing import Any
import inspect
import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .client_async import (
    ASYNC_ITERATOR,
    CANCELLED_TASK,
    CLOSE_SENTINEL,
    CREATE_OBJECT,
    DELETE_OBJECT,
    EXCEPTION,
    FETCH_OBJECT,
    GET_ATTRIBUTE,
    ITER_ASYNC_ITERATOR,
    MOVE_ASYNC_ITERATOR,
    METHOD,
    OK,
    SET_ATTRIBUTE,
    USER_EXCEPTION,
)
from .common import UserException, cancel_task_group_on_signal, scoped_insert
from .connection import TCPConnection


async def wrap_sync_generator(sync_generator):
    for value in sync_generator:
        yield value


async def wrap_coroutine(coroutine):
    return OK, await coroutine


class ClientSession:
    def __init__(self, server, task_group, connection: Connection) -> None:
        self.session_manager: Server = server
        self.task_group = task_group
        self.connection = connection
        self.running_tasks = {}
        self.pending_generators = {}
        self.own_objects = set()
        self.synchronization_indexes = {}

    async def send(self, code: str, request_id: int, status: str, value: Any):
        await self.connection.send((code, request_id, status, value))

    def send_nowait(self, code: str, request_id: int, status: str, value: Any):
        self.connection.send_nowait((code, request_id, status, value))

    async def iterate_through_async_generator_unsync(
        self, request_id: int, iterator_id: int, coroutine_or_async_generator
    ):
        async with asyncstdlib.scoped_iter(coroutine_or_async_generator) as aiter:
            async for value in aiter:
                await self.send(ITER_ASYNC_ITERATOR, request_id, OK, value)
        return CLOSE_SENTINEL, None

    async def iterate_through_async_generator_sync(
        self, request_id: int, iterator_id: int, coroutine_or_async_generator
    ):
        index_and_event = [0, anyio.Event()]
        with scoped_insert(self.synchronization_indexes, iterator_id, index_and_event):
            async with asyncstdlib.scoped_iter(coroutine_or_async_generator) as aiter:
                async for index, value in asyncstdlib.enumerate(aiter):
                    await self.send(ITER_ASYNC_ITERATOR, request_id, OK, value)
                    if index >= index_and_event[0]:
                        await index_and_event[1].wait()
            return CLOSE_SENTINEL, None

    def iterate_generator(self, request_id: int, iterator_id: int):
        if not (generator := self.pending_generators.pop(iterator_id, None)):
            return
        push, generator = generator
        method = (
            self.iterate_through_async_generator_unsync
            if push
            else self.iterate_through_async_generator_sync
        )
        self.cancellable_run_task(
            request_id, ITER_ASYNC_ITERATOR, method(request_id, iterator_id, generator)
        )

    async def run_task(self, request_id, task_code, coroutine_or_async_generator):
        status, result = EXCEPTION, None
        try:
            status, result = await coroutine_or_async_generator
        except anyio.get_cancelled_exc_class():
            status = CANCELLED_TASK
            raise
        except UserException as e:
            status, result = USER_EXCEPTION, e.args[0]
        except Exception:
            status, result = EXCEPTION, traceback.format_exc()
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(task_code, request_id, status, result)

    def cancellable_run_task(self, request_id, task_code, coroutine_or_async_context):
        async def task():
            task_group = anyio.create_task_group()
            with scoped_insert(self.running_tasks, request_id, task_group.cancel_scope.cancel):
                async with task_group:
                    task_group.start_soon(
                        self.run_task,
                        request_id,
                        task_code,
                        coroutine_or_async_context,
                    )

        self.task_group.start_soon(task)

    async def create_object(self, request_id, object_class, args, kwarg):
        object_id = next(self.session_manager.object_id)
        code, message = OK, object_id
        try:
            self.session_manager.objects[object_id] = object_class(
                self.session_manager.server_object, *args, **kwarg
            )
            self.own_objects.add(object_id)
        except Exception:
            code, message = EXCEPTION, traceback.format_exc()
        await self.send(CREATE_OBJECT, request_id, code, message)

    async def fetch_object(self, request_id, object_id):
        maybe_object = self.session_manager.objects.get(object_id)
        if maybe_object is not None:
            await self.send(FETCH_OBJECT, request_id, OK, maybe_object.__class__)
        else:
            await self.send(FETCH_OBJECT, request_id, EXCEPTION, f"Object {object_id} not found")

    async def get_attribute(self, request_id, object_id, name):
        code, value = OK, None
        try:
            value = getattr(self.session_manager.objects[object_id], name)
        except Exception as e:
            code, value = EXCEPTION, e
        await self.send(GET_ATTRIBUTE, request_id, code, value)

    async def set_attribute(self, request_id, object_id, name, value):
        code, result = OK, None
        try:
            setattr(self.session_manager.objects[object_id], name, value)
        except Exception as e:
            code, result = EXCEPTION, e
        await self.send(SET_ATTRIBUTE, request_id, code, result)

    async def cancel_running_task(self, request_id: int):
        if running_task := self.running_tasks.get(request_id):
            running_task()

    def move_async_generator_index(self, request_id: int, index: int):
        if index_and_event := self.synchronization_indexes.get(request_id, None):
            index_and_event[1].set()
            index_and_event[0] = index
            index_and_event[1] = anyio.Event()

    async def evaluate_method(self, request_id, task_code, object_id, method, args, kwargs):
        result = method(self.session_manager.objects[object_id], *args, **kwargs)
        if inspect.iscoroutine(result):
            self.cancellable_run_task(request_id, task_code, wrap_coroutine(result))
        elif inspect.isasyncgen(result) or inspect.isgenerator(result):
            is_async = inspect.isasyncgen(result)
            self.pending_generators[request_id] = (is_async, result)
            await self.send(task_code, request_id, ASYNC_ITERATOR, (is_async, request_id))
        else:
            await self.send(task_code, request_id, OK, result)

    async def process_messages(self):
        async for task_code, request_id, payload in self.connection:
            try:
                if task_code == METHOD:
                    await self.evaluate_method(request_id, task_code, *payload)
                elif task_code == CANCELLED_TASK:
                    await self.cancel_running_task(request_id)
                elif task_code == ITER_ASYNC_ITERATOR:
                    self.iterate_generator(request_id, *payload)
                elif task_code == GET_ATTRIBUTE:
                    await self.get_attribute(request_id, *payload)
                elif task_code == SET_ATTRIBUTE:
                    await self.set_attribute(request_id, *payload)
                elif task_code == CREATE_OBJECT:
                    await self.create_object(request_id, *payload)
                elif task_code == FETCH_OBJECT:
                    await self.fetch_object(request_id, payload)
                elif task_code == MOVE_ASYNC_ITERATOR:
                    self.move_async_generator_index(request_id, *payload)
                elif task_code == DELETE_OBJECT:
                    self.session_manager.objects.pop(payload, None)
                    self.own_objects.discard(payload)
                else:
                    raise Exception(f"Unknown code {repr(task_code)} with payload {repr(payload)}")
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                stack = traceback.format_exc()
                await self.send(task_code, request_id, EXCEPTION, stack)

    async def aclose(self):
        self.task_group.cancel_scope.cancel()
        for object_id in self.own_objects:
            self.session_manager.objects.pop(object_id, None)


class Server:
    def __init__(self, server_object: Any) -> None:
        self.server_object = server_object
        self.client_sessions = {}
        self.client_session_id = count()
        self.object_id = count()
        self.objects = {next(self.object_id): server_object}

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        async with anyio.create_task_group() as session_task_group:
            client_session = ClientSession(self, session_task_group, connection)
            with scoped_insert(self.client_sessions, next(self.client_session_id), client_session):
                async with asyncstdlib.closing(client_session):
                    yield client_session


async def _serve_tcp(port: int, server_object: Any):
    session_manager = Server(server_object)

    async def on_new_connection_raw(reader, writer):
        async with session_manager.on_new_connection(
            TCPConnection(reader, writer, throw_on_eof=False)
        ) as client_core:
            await client_core.process_messages()

    async with await asyncio.start_server(on_new_connection_raw, "localhost", port) as tcp_server:
        await tcp_server.serve_forever()


async def handle_signals(main, *args, **kwargs):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(cancel_task_group_on_signal, task_group)
        task_group.start_soon(main, *args, **kwargs)


async def start_tcp_server(port: int, server_object: Any):
    await handle_signals(_serve_tcp, port, server_object)


def run_tcp_server(port: int, server_object: Any):
    anyio.run(start_tcp_server, port, server_object)
