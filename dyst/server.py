import asyncio
import contextlib
import logging
import signal
import sys
import traceback
from typing import Any, Tuple

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .client_async import (
    ASYNC_GENERATOR,
    CANCELLED_TASK,
    CLOSE_SENTINEL,
    COROUTINE,
    CREATE_OBJECT,
    EXCEPTION,
    GET_ATTRIBUTE,
    OK,
    USER_EXCEPTION,
)
from .common import scoped_insert, UserException
from .connection import TCPConnection


if sys.version_info < (3, 10):
    from asyncstdlib import anext


class ClientSession:
    def __init__(self, server, task_group, name, connection: Connection) -> None:
        self.server = server
        self.task_group = task_group
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.objects = {}

    def __str__(self) -> str:
        return self.name

    async def send(self, code: str, request_id: int, args: Tuple[Any, ...]):
        await self.connection.send((code, request_id, args))

    async def cancellable_task(self, request_id, task_type, coroutine_or_async_context):
        code, result = CLOSE_SENTINEL, None
        try:
            if task_type == ASYNC_GENERATOR:
                async with asyncstdlib.scoped_iter(coroutine_or_async_context) as aiter:
                    async for value in aiter:
                        await self.send(task_type, request_id, (OK, value))
            else:
                code, result = OK, await coroutine_or_async_context

        except anyio.get_cancelled_exc_class():
            code = CANCELLED_TASK
            raise
        except UserException as e:
            code, result = USER_EXCEPTION, e.args[0]
        except Exception:
            code, result = EXCEPTION, traceback.format_exc()
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(task_type, request_id, (code, result))

    async def cancellable_task_runner(self, request_id, task_type, coroutine_or_async_context):
        try:
            async with anyio.create_task_group() as self.running_tasks[request_id]:
                self.running_tasks[request_id].start_soon(
                    self.cancellable_task,
                    request_id,
                    task_type,
                    coroutine_or_async_context,
                )
        finally:
            self.running_tasks.pop(request_id, None)

    async def create_object(self, request_id, object_id, object_class, args, kwarg):
        code, message = OK, None
        if object_class:
            try:
                self.objects[object_id] = object_class(self.server, *args, **kwarg)
            except Exception:
                code, message = EXCEPTION, traceback.format_exc()
            await self.send(CREATE_OBJECT, request_id, (code, message))
        else:
            self.objects.pop(object_id, None)

    async def process_messages(self):
        async for code, request_id, payload in self.connection:
            if code in (COROUTINE, ASYNC_GENERATOR):
                if payload:
                    object_id, command, args, kwargs = payload
                    coroutine_or_async_context = getattr(self.objects[object_id], command)(
                        *args, **kwargs
                    )
                    self.task_group.start_soon(
                        self.cancellable_task_runner,
                        request_id,
                        code,
                        coroutine_or_async_context,
                    )
                else:
                    request_task_group = self.running_tasks.get(request_id)
                    if request_task_group:
                        logging.info(f"{self.name} cancelling task {request_id}")
                        request_task_group.cancel_scope.cancel()
            elif code == GET_ATTRIBUTE:
                object_id, name = payload
                await self.send(
                    GET_ATTRIBUTE, request_id, (OK, getattr(self.objects[object_id], name))
                )
            elif code == CREATE_OBJECT:
                await self.create_object(request_id, *payload)
            else:
                raise Exception(f"Unknown code {code} with payload {payload}")

    async def aclose(self):
        self.task_group.cancel_scope.cancel()


class SessionManager:
    def __init__(self, server: Any) -> None:
        self.sessions = {}
        self.server = server

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        client_name = "unknown"
        try:
            (client_name,) = await anext(connection)
            logging.info(f"{client_name} connected")
            async with anyio.create_task_group() as task_group:
                connection_session = ClientSession(
                    self.server, task_group, client_name, connection
                )
                with scoped_insert(self.sessions, client_name, connection_session):
                    async with asyncstdlib.closing(connection_session):
                        yield connection_session
        except anyio.get_cancelled_exc_class():
            raise
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Internal error: {e}") from e
        finally:
            logging.info(f"{client_name} disconnected")


async def signal_handler(scope: anyio.abc.CancelScope):
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                print("Ctrl+C pressed!")
            else:
                print("Terminated!")

            scope.cancel()
            return


async def _serve_tcp(port, server):
    session_manager = SessionManager(server)

    async def on_new_connection_raw(reader, writer):
        async with session_manager.on_new_connection(
            TCPConnection(reader, writer, throw_on_eof=False)
        ) as client_core:
            await client_core.process_messages()

    async with await asyncio.start_server(on_new_connection_raw, "localhost", port) as tcp_server:
        await tcp_server.serve_forever()


async def handle_signals(main, *args, **kwargs):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(signal_handler, task_group.cancel_scope)
        task_group.start_soon(main, *args, **kwargs)


async def start_tcp_server(port, server):
    await handle_signals(_serve_tcp, port, server)

