import asyncio
import contextlib
import traceback
from itertools import count
from typing import Any

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .client_async import (
    ASYNC_ITERATOR,
    AWAITABLE,
    CANCELLED_TASK,
    CLOSE_SENTINEL,
    CREATE_OBJECT,
    DELETE_OBJECT,
    EXCEPTION,
    FETCH_OBJECT,
    FUNCTION,
    GET_ATTRIBUTE,
    ITER_ASYNC_ITERATOR,
    OK,
    USER_EXCEPTION,
)
from .common import (
    UserException,
    cancel_task_group_on_signal,
    cancel_task_on_exit,
    execute_cancellable_coroutine,
    print_error_stack,
    scoped_insert,
)
from .connection import TCPConnection


class ClientSession:
    def __init__(self, server, task_group, connection: Connection) -> None:
        self.session_manager: "SessionManager" = server
        self.task_group = task_group
        self.connection = connection
        self.running_tasks = {}
        self.pending_async_generators = {}
        self.own_objects = set()

    async def send(self, code: str, request_id: int, status: str, value: Any):
        await self.connection.send((code, request_id, status, value))

    def send_nowait(self, code: str, request_id: int, status: str, value: Any):
        self.connection.send_nowait((code, request_id, status, value))

    async def evaluate_coroutine_or_async_generator(
        self, request_id, task_type, coroutine_or_async_generator
    ):
        code, result = CLOSE_SENTINEL, None
        try:
            if asyncio.iscoroutine(coroutine_or_async_generator):
                code, result = AWAITABLE, await coroutine_or_async_generator
            else:
                async with asyncstdlib.scoped_iter(coroutine_or_async_generator) as aiter:
                    async for value in aiter:
                        await self.send(task_type, request_id, OK, value)

        except anyio.get_cancelled_exc_class():
            code = CANCELLED_TASK
            raise
        except UserException as e:
            code, result = USER_EXCEPTION, e.args[0]
        except Exception:
            code, result = EXCEPTION, traceback.format_exc()
            raise
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(task_type, request_id, code, result)

    async def run_task(self, request_id, task_type, coroutine_or_async_context):
        try:
            async with execute_cancellable_coroutine(
                self.evaluate_coroutine_or_async_generator,
                request_id,
                task_type,
                coroutine_or_async_context,
            ) as self.running_tasks[request_id]:
                pass
        finally:
            self.running_tasks.pop(request_id, None)

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
        code, message = OK, None
        try:
            value = getattr(self.session_manager.objects[object_id], name)
            await self.send(GET_ATTRIBUTE, request_id, OK, value)
        except Exception as e:
            code, message = EXCEPTION, e
        await self.send(GET_ATTRIBUTE, request_id, code, message)

    async def cancel_running_task(self, request_id: int):
        running_task = self.running_tasks.get(request_id)
        if running_task:
            running_task.cancel()
            await running_task

    async def process_messages(self):
        async for code, request_id, payload in self.connection:
            try:
                if code in (FUNCTION, AWAITABLE):
                    object_id, function, args, kwargs = payload
                    coroutine_or_async_context = function(
                        self.session_manager.objects[object_id], *args, **kwargs
                    )
                    if code != FUNCTION or asyncio.iscoroutine(coroutine_or_async_context):
                        self.task_group.start_soon(
                            self.run_task,
                            request_id,
                            code,
                            coroutine_or_async_context,
                        )
                    else:
                        self.pending_async_generators[request_id] = coroutine_or_async_context
                        await self.send(code, request_id, ASYNC_ITERATOR, request_id)
                elif code == CANCELLED_TASK:
                    await self.cancel_running_task(request_id)
                elif code == ITER_ASYNC_ITERATOR:
                    self.task_group.start_soon(
                        self.run_task,
                        request_id,
                        code,
                        self.pending_async_generators.pop(payload),
                    )
                elif code == GET_ATTRIBUTE:
                    await self.get_attribute(request_id, *payload)
                elif code == CREATE_OBJECT:
                    await self.create_object(request_id, *payload)
                elif code == FETCH_OBJECT:
                    await self.fetch_object(request_id, payload)
                elif code == DELETE_OBJECT:
                    self.session_manager.objects.pop(payload, None)
                    self.own_objects.discard(payload)
                else:
                    raise Exception(f"Unknown code {repr(code)} with payload {repr(payload)}")
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                stack = traceback.format_exc()
                await self.send(code, request_id, EXCEPTION, stack)

    async def aclose(self):
        self.task_group.cancel_scope.cancel()
        for object_id in self.own_objects:
            self.session_manager.objects.pop(object_id, None)


class SessionManager:
    def __init__(self, server_object: Any) -> None:
        self.server_object = server_object
        self.client_sessions = {}
        self.client_session_id = count()
        self.object_id = count()
        self.objects = {next(self.object_id): server_object}

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        with print_error_stack():
            async with anyio.create_task_group() as task_group:
                client_session = ClientSession(self, task_group, connection)
                with scoped_insert(
                    self.client_sessions, next(self.client_session_id), client_session
                ):
                    async with asyncstdlib.closing(client_session):
                        yield client_session


async def _serve_tcp(port: int, server_object: Any):
    session_manager = SessionManager(server_object)

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
