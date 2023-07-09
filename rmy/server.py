from __future__ import annotations

import asyncio
import contextlib
import inspect
import sys
import traceback
from itertools import count
from typing import Any, Dict
import io

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .client_async import (
    AWAIT_COROUTINE,
    CANCEL_TASK,
    CLOSE_SENTINEL,
    CREATE_OBJECT,
    DELETE_OBJECT,
    EVALUATE_METHOD,
    EXCEPTION,
    FETCH_OBJECT,
    GET_ATTRIBUTE,
    ITERATE_GENERATOR,
    MOVE_GENERATOR_ITERATOR,
    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
    OK,
    SET_ATTRIBUTE,
    RemoteCoroutine,
    RemoteGeneratorPull,
    RemoteGeneratorPush,
    RMY_Pickler,
)
from .common import RemoteException, cancel_task_group_on_signal, scoped_insert
from .connection import TCPConnection

MAX_DATA_SIZE_IN_FLIGHT = 1_000
MAX_DATA_NB_IN_FLIGHT = 10


async def wrap_sync_generator(sync_generator):
    for value in sync_generator:
        yield value


async def wrap_coroutine(coroutine):
    return OK, await coroutine


class GeneratorState:
    def __init__(self):
        self.messages_in_flight_total_size = 0
        self.nb_messages_in_flight = 0
        self.acknowledged_message = anyio.Event()


class ClientSession:
    def __init__(self, server: Server, task_group, connection: Connection) -> None:
        self.server: Server = server
        self.task_group = task_group
        self.connection = connection
        connection.set_dumps(self.dumps)
        self.tasks_cancel_callbacks = {}
        self.pending_results = {}
        self.own_objects = set()
        self.generator_states: Dict[int, GeneratorState] = {}
        self.value_id = count(10)

    def dumps(self, value):
        file = io.BytesIO()
        RMY_Pickler(self, file).dump(value)
        return file.getvalue()

    async def send(self, code: str, request_id: int, status: str, value: Any) -> int:
        return await self.connection.send((code, request_id, status, value))

    def send_nowait(self, code: str, request_id: int, status: str, value: Any) -> int:
        return self.connection.send_nowait((code, request_id, status, value))

    async def iterate_through_async_generator(
        self, request_id: int, iterator_id: int, coroutine_or_async_generator, pull_or_push: bool
    ):
        generator_state = GeneratorState()
        with scoped_insert(self.generator_states, iterator_id, generator_state):
            async with asyncstdlib.scoped_iter(coroutine_or_async_generator) as aiter:
                async for value in aiter:
                    message_size = await self.send(ITERATE_GENERATOR, request_id, OK, value)
                    generator_state.messages_in_flight_total_size += message_size
                    generator_state.nb_messages_in_flight += 1
                    if (
                        generator_state.messages_in_flight_total_size
                        > self.server.max_data_size_in_flight
                        or generator_state.nb_messages_in_flight
                        > self.server.max_data_nb_in_flight
                    ):
                        if pull_or_push:
                            await self.send(ITERATE_GENERATOR, request_id, "PULL", None)
                        else:
                            raise OverflowError(
                                " ".join(
                                    [
                                        ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
                                        f"Current data size in flight {generator_state.messages_in_flight_total_size}, max is {MAX_DATA_SIZE_IN_FLIGHT}.",
                                        f"Current number of messages in flight: {generator_state.nb_messages_in_flight}, max is {MAX_DATA_NB_IN_FLIGHT}.",
                                    ]
                                )
                            )
                        await generator_state.acknowledged_message.wait()
            return CLOSE_SENTINEL, None

    def iterate_generator(self, request_id: int, iterator_id: int, pull_or_push: bool):
        if not (generator := self.pending_results.pop(iterator_id, None)):
            return
        self.cancellable_run_task(
            request_id,
            ITERATE_GENERATOR,
            self.iterate_through_async_generator(request_id, iterator_id, generator, pull_or_push),
        )

    def evaluate_coroutine(self, request_id: int, coroutine_id: int):
        if not (coroutine := self.pending_results.pop(coroutine_id, None)):
            return
        self.cancellable_run_task(request_id, AWAIT_COROUTINE, wrap_coroutine(coroutine))

    async def run_task(self, request_id, task_code, coroutine_or_async_generator):
        status, result = EXCEPTION, None
        try:
            status, result = await coroutine_or_async_generator
        except anyio.get_cancelled_exc_class():
            status = CANCEL_TASK
            raise
        except Exception as e:
            _, e, tb = sys.exc_info()
            status, result = EXCEPTION, RemoteException(e, traceback.extract_tb(tb)[3:])
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(task_code, request_id, status, result)

    def cancellable_run_task(self, request_id, task_code, coroutine_or_async_context):
        async def task():
            task_group = anyio.create_task_group()
            with scoped_insert(
                self.tasks_cancel_callbacks, request_id, task_group.cancel_scope.cancel
            ):
                async with task_group:
                    task_group.start_soon(
                        self.run_task,
                        request_id,
                        task_code,
                        coroutine_or_async_context,
                    )

        self.task_group.start_soon(task)

    async def create_object(self, request_id, object_class, args, kwarg):
        object_id = next(self.server.object_id)
        code, message = OK, object_id
        try:
            self.server.objects[object_id] = object_class(
                self.server.server_object, *args, **kwarg
            )
            self.own_objects.add(object_id)
        except Exception:
            code, message = EXCEPTION, traceback.format_exc()
        await self.send(CREATE_OBJECT, request_id, code, message)

    async def fetch_object(self, request_id, object_id):
        maybe_object = self.server.objects.get(object_id)
        if maybe_object is not None:
            await self.send(FETCH_OBJECT, request_id, OK, maybe_object.__class__)
        else:
            await self.send(FETCH_OBJECT, request_id, EXCEPTION, f"Object {object_id} not found")

    async def get_attribute(self, request_id, object_id, name):
        code, value = OK, None
        try:
            value = getattr(self.server.objects[object_id], name)
        except Exception as e:
            code, value = EXCEPTION, e
        await self.send(GET_ATTRIBUTE, request_id, code, value)

    async def set_attribute(self, request_id, object_id, name, value):
        code, result = OK, None
        try:
            setattr(self.server.objects[object_id], name, value)
        except Exception as e:
            code, result = EXCEPTION, e
        await self.send(SET_ATTRIBUTE, request_id, code, result)

    async def cancel_task(self, request_id: int):
        if running_task_cancel_callback := self.tasks_cancel_callbacks.get(request_id):
            running_task_cancel_callback()

    def move_async_generator_index(self, request_id: int, message_size: int):
        if generator_state := self.generator_states.get(request_id):
            generator_state.messages_in_flight_total_size -= message_size
            generator_state.nb_messages_in_flight -= 1
            generator_state.acknowledged_message.set()

    async def evaluate_method(self, request_id, object_id, method, args, kwargs):
        result = method(self.server.objects[object_id], *args, **kwargs)
        if inspect.iscoroutine(result):
            result = RemoteCoroutine(result)
        elif inspect.isasyncgen(result):
            result = RemoteGeneratorPush(result)
        elif inspect.isgenerator(result):
            result = RemoteGeneratorPull(result)
        await self.send(EVALUATE_METHOD, request_id, OK, result)

    def store_value(self, value: Any):
        value_id = next(self.value_id)
        self.pending_results[value_id] = value
        return value_id

    async def process_messages(self):
        async for task_code, request_id, payload in self.connection:
            try:
                if task_code == EVALUATE_METHOD:
                    await self.evaluate_method(request_id, *payload)
                elif task_code == CANCEL_TASK:
                    await self.cancel_task(request_id)
                elif task_code == ITERATE_GENERATOR:
                    self.iterate_generator(request_id, *payload)
                elif task_code == AWAIT_COROUTINE:
                    self.evaluate_coroutine(request_id, *payload)
                elif task_code == GET_ATTRIBUTE:
                    await self.get_attribute(request_id, *payload)
                elif task_code == SET_ATTRIBUTE:
                    await self.set_attribute(request_id, *payload)
                elif task_code == CREATE_OBJECT:
                    await self.create_object(request_id, *payload)
                elif task_code == FETCH_OBJECT:
                    await self.fetch_object(request_id, *payload)
                elif task_code == MOVE_GENERATOR_ITERATOR:
                    self.move_async_generator_index(request_id, *payload)
                elif task_code == DELETE_OBJECT:
                    self.server.objects.pop(payload, None)
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
            self.server.objects.pop(object_id, None)


class Server:
    def __init__(
        self,
        server_object: Any,
        max_data_size_in_flight=MAX_DATA_SIZE_IN_FLIGHT,
        max_data_nb_in_flight=MAX_DATA_NB_IN_FLIGHT,
    ) -> None:
        self.server_object = server_object
        self.client_sessions = {}
        self.client_session_id = count()
        self.object_id = count()
        self.objects = {next(self.object_id): server_object}
        self.max_data_size_in_flight = max_data_size_in_flight
        self.max_data_nb_in_flight = max_data_nb_in_flight

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
