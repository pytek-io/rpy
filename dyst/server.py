import contextlib
import inspect
import logging
import traceback
from collections import defaultdict
from typing import Any, Dict, Set

import anyio
import asyncstdlib

from .abc import Connection
from .common import anext
from .connection import TCPConnection


SUBSCRIPTION_BUFFER_SIZE = 100
OVERRIDE_ERROR_MESSAGE = "Trying to override an existing event without override set to True."


class UserException(Exception):
    """Use this to signal expected errors to users."""

    pass


class ClientSessionBase:
    """Implements non functional specific details."""

    def __init__(self, server, task_group, name, connection: Connection) -> None:
        self.task_group = task_group
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.server: ServerBase = server
        self.client_methods = {}

    def __str__(self) -> str:
        return self.name

    async def send(self, *args: Any):
        await self.connection.send(args)

    @contextlib.contextmanager
    def subscribe(self, topic: Any):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.server.subscriptions[topic]
        try:
            subscriptions.add(sink)
            yield stream
        finally:
            if sink in subscriptions:
                subscriptions.remove(sink)

    def broadcast_to_subscrptions(self, topic: Any, message: Any):
        for subscription in self.server.subscriptions[topic]:
            try:
                subscription.send_nowait(message)
            except anyio.WouldBlock:
                logging.warning("ignoring subscriber which is too far behind")

    async def evaluate_command(self, request_id, coroutine):
        send_termination = True
        try:
            success, result = True, await coroutine
        except anyio.get_cancelled_exc_class():
            send_termination = False
            raise
        except UserException as e:
            success, result = False, e.args[0]
        except Exception:
            success, result = False, traceback.format_exc()
        if send_termination:
            await self.send(request_id, success, result)

    async def evaluate_stream(self, request_id, stream):
        success, result = True, None
        try:
            async for result in stream:
                await self.send(request_id, True, result)
        except anyio.get_cancelled_exc_class():
            raise
        except Exception:
            success, result = False, traceback.format_exc()
        finally:
            await self.send(request_id, success, result)

    async def cancellable_task_runner(self, request_id, command, details):
        try:
            async with anyio.create_task_group() as self.running_tasks[request_id]:
                coroutine_or_async_context = self.client_methods[command](*details)
                self.running_tasks[request_id].start_soon(
                    self.evaluate_command
                    if inspect.isawaitable(coroutine_or_async_context)
                    else self.evaluate_stream,
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
                    await request_task_group.cancel_scope.cancel()
            else:
                self.task_group.start_soon(
                    self.cancellable_task_runner, request_id, command, details
                )

    async def aclose(self):
        await self.task_group.cancel_scope.cancel()


class ServerBase:
    def __init__(self, client_session_type, task_group) -> None:
        self.task_group = task_group
        self.subscriptions: Dict[str, Set] = defaultdict(set)
        self.client_session_type = client_session_type

    async def on_new_connection(self, connection: Connection):
        client_name = "unknown"
        try:
            (client_name,) = await anext(connection)
            logging.info(f"{client_name} connected")
            async with anyio.create_task_group() as task_group:
                client_core = ClientSessionBase(self, task_group, client_name, connection)
                client_session = self.client_session_type(self, client_core)
                client_core.client_methods.update(
                    (attr, getattr(client_session, attr)) for attr in dir(client_session)
                )
                async with asyncstdlib.closing(client_core):
                    await client_core.process_messages()
        except Exception:
            # Catching internal issues here, should never get there.
            traceback.print_exc()
        finally:
            logging.info(f"{client_name} disconnected")

    async def on_new_connection_raw(self, reader, writer):
        await self.on_new_connection(TCPConnection(reader, writer, False))
