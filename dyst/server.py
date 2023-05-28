import contextlib
import inspect
import logging
import sys
import traceback
from collections import defaultdict
from typing import Any, Dict, Set

import anyio
import asyncstdlib

from .abc import Connection
from .client_async import CANCELLED_TASK, CLOSE_SENTINEL, EXCEPTION, OK, USER_EXCEPTION
from .connection import TCPConnection
from .exception import UserException


if sys.version_info < (3, 10):
    from asyncstdlib import anext

SUBSCRIPTION_BUFFER_SIZE = 100
OVERRIDE_ERROR_MESSAGE = "Trying to overwrite an existing event without overwrite set to True."


class ClientSessionBase:
    """Implements non functional specific details."""

    def __init__(self, task_group, name, connection: Connection) -> None:
        self.task_group = task_group
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.client_methods = {}

    def __str__(self) -> str:
        return self.name

    async def send(self, *args: Any):
        await self.connection.send(args)

    async def cancellable_task(self, request_id, coroutine_or_async_context):
        code, result = CLOSE_SENTINEL, None
        try:
            if inspect.isawaitable(coroutine_or_async_context):
                code, result = OK, await coroutine_or_async_context
            else:
                async for result in coroutine_or_async_context:
                    await self.send(request_id, OK, result)
        except anyio.get_cancelled_exc_class():
            code = CANCELLED_TASK
            raise
        except UserException as e:
            code, result = USER_EXCEPTION, e.args[0]
        except Exception:
            code, result = EXCEPTION, traceback.format_exc()
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(request_id, code, result)

    async def cancellable_task_runner(self, request_id, command, details):
        try:
            async with anyio.create_task_group() as self.running_tasks[request_id]:
                coroutine_or_async_context = self.client_methods[command](*details)
                self.running_tasks[request_id].start_soon(
                    self.cancellable_task,
                    request_id,
                    coroutine_or_async_context,
                )
        finally:
            self.running_tasks.pop(request_id, None)

    async def process_messages(self):
        async for request_id, command, details in self.connection:
            if command is None:
                request_task_group = self.running_tasks.get(request_id)
                logging.info(f"{self.name} cancelling subscription {request_id}")
                if request_task_group:
                    request_task_group.cancel_scope.cancel()
            else:
                self.task_group.start_soon(
                    self.cancellable_task_runner, request_id, command, details
                )

    async def aclose(self):
        self.task_group.cancel_scope.cancel()


class ServerBase:
    def __init__(self, client_session_type, task_group) -> None:
        self.task_group = task_group
        self.subscriptions: Dict[str, Set] = defaultdict(set)
        self.client_session_type = client_session_type
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
                client_core = ClientSessionBase(task_group, client_name, connection)
                with self.register_session(
                    client_name, self.client_session_type(self, client_name)
                ) as client_session:
                    client_core.client_methods.update(
                        (attr, getattr(client_session, attr)) for attr in dir(client_session)
                    )
                    async with asyncstdlib.closing(client_core):
                        yield client_core
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
