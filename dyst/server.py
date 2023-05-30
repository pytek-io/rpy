import contextlib
import inspect
import logging
import sys
import traceback
from collections import defaultdict
from typing import Any, Dict, Set, Tuple
import asyncstdlib


import anyio
import asyncstdlib
from .abc import Connection
from .client_async import (
    CANCELLED_TASK,
    CLOSE_SENTINEL,
    EXCEPTION,
    OK,
    USER_EXCEPTION,
    COMMAND,
    CREATE_OBJECT,
    RESULT,
)
from .connection import TCPConnection
from .exception import UserException


if sys.version_info < (3, 10):
    from asyncstdlib import anext

SUBSCRIPTION_BUFFER_SIZE = 100
OVERRIDE_ERROR_MESSAGE = "Trying to overwrite an existing event without overwrite set to True."


class ConnectionSession:
    def __init__(self, server, task_group, name, connection: Connection) -> None:
        self.server = server
        self.task_group = task_group
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.objects = {}

    def __str__(self) -> str:
        return self.name

    async def send(self, code: str, args: Tuple[Any, ...]):
        await self.connection.send((code, args))

    async def cancellable_task(self, request_id, coroutine_or_async_context):
        code, result = CLOSE_SENTINEL, None
        try:
            if inspect.isawaitable(coroutine_or_async_context):
                code, result = OK, await coroutine_or_async_context
            else:
                async with asyncstdlib.scoped_iter(coroutine_or_async_context) as aiter:
                    async for value in aiter:
                        await self.send(RESULT, (request_id, OK, value))
        except anyio.get_cancelled_exc_class():
            code = CANCELLED_TASK
            raise
        except UserException as e:
            code, result = USER_EXCEPTION, e.args[0]
        except Exception:
            code, result = EXCEPTION, traceback.format_exc()
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(RESULT, (request_id, code, result))

    async def cancellable_task_runner(self, request_id, attr, *args):
        try:
            if callable(attr):
                coroutine_or_async_context = attr(*args)
            else:
                coroutine_or_async_context = attr
            async with anyio.create_task_group() as self.running_tasks[request_id]:
                self.running_tasks[request_id].start_soon(
                    self.cancellable_task,
                    request_id,
                    coroutine_or_async_context,
                )
        finally:
            self.running_tasks.pop(request_id, None)

    async def create_object(self, object_id, object_class, args, kwarg):
        code, message = OK, None
        if object_class:
            try:
                self.objects[object_id] = object_class(self.server, *args, **kwarg)
            except Exception:
                code, message = EXCEPTION, traceback.format_exc()
            await self.send(CREATE_OBJECT, (object_id, code, message))
        else:
            self.objects.pop(object_id, None)

    async def process_messages(self):
        async for code, payload in self.connection:
            if code == COMMAND:
                object_id, request_id, command, args = payload
                if command is None:
                    request_task_group = self.running_tasks.get(request_id)
                    if request_task_group:
                        logging.info(f"{self.name} cancelling subscription {request_id}")
                        request_task_group.cancel_scope.cancel()
                else:
                    attr = getattr(self.objects[object_id], command)
                    self.task_group.start_soon(
                        self.cancellable_task_runner,
                        request_id,
                        attr,
                        *args,
                    )
            elif code == CREATE_OBJECT:
                await self.create_object(*payload)
            else:
                raise Exception(f"Unknown code {code} with payload {payload}")

    async def aclose(self):
        self.task_group.cancel_scope.cancel()


class ServerBase:
    def __init__(self, task_group) -> None:
        self.task_group = task_group
        self.subscriptions: Dict[str, Set] = defaultdict(set)
        self.sessions = {}

    @contextlib.contextmanager
    def register_session(self, client_name, client_session):
        try:
            self.sessions[client_name] = client_session
            yield client_session
        finally:
            self.sessions.pop(client_name, None)

    @contextlib.contextmanager
    def subscribe(self, topic: Any):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.subscriptions[topic]
        try:
            subscriptions.add(sink)
            yield stream
        finally:
            if sink in subscriptions:
                subscriptions.remove(sink)

    def broadcast_to_subscriptions(self, topic: Any, message: Any):
        for subscription in self.subscriptions[topic]:
            try:
                subscription.send_nowait(message)
            except anyio.WouldBlock:
                logging.warning("ignoring subscriber which is too far behind")

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        client_name = "unknown"
        try:
            (client_name,) = await anext(connection)
            logging.info(f"{client_name} connected")
            async with anyio.create_task_group() as task_group:
                connection_session = ConnectionSession(self, task_group, client_name, connection)
                with self.register_session(client_name, connection_session):
                    async with asyncstdlib.closing(connection_session):
                        yield connection_session
        except Exception:
            # Catching internal issues here, should never get there.
            traceback.print_exc()
            raise
        finally:
            logging.info(f"{client_name} disconnected")

    async def on_new_connection_raw(self, reader, writer):
        async with self.on_new_connection(
            TCPConnection(reader, writer, throw_on_eof=False)
        ) as client_core:
            await client_core.process_messages()
