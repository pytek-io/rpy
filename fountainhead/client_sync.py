import asyncio
import contextlib
import inspect
import threading
import traceback
from datetime import datetime
from functools import partial, wraps
from itertools import count
from typing import Any, Iterator, Optional

import anyio
import janus

from .client_async import (CANCEL_TASK, CLOSE_SENTINEL, EXCEPTION, OK,
                           SHUTDOWN, START_TASK)
from .client_async import Client as AsyncClient
from .client_async import _create_async_client
from .common import async_null_context
from .connection import wrap_websocket_connection


def evaluate_in_background_thread(method):
    @wraps(method)
    def _result(self: "Client", *args, **kwargs):
        task_id = next(self.task_id)
        self.to_background.sync_q.put((START_TASK, (task_id, method, args, kwargs)))
        success, value = self.from_background.sync_q.get()
        if success:
            return value
        raise value

    return _result


def evaluate_context_generator_in_background_thread(method):
    @contextlib.contextmanager
    def _result(self: "Client", *args, **kwargs):
        task_id = next(self.task_id)
        self.to_background.sync_q.put((START_TASK, (task_id, method, args, kwargs)))

        @wraps(method)
        def result():
            while True:
                m = self.from_background.sync_q.get()
                code, value = m
                if code is CLOSE_SENTINEL:
                    break
                if code is EXCEPTION:
                    raise value
                yield value

        try:
            yield result()
        finally:
            self.to_background.sync_q.put((CANCEL_TASK, task_id))

    return _result


class Client:
    def __init__(self, async_client: AsyncClient) -> None:
        self.from_background = janus.Queue()
        self.to_background = janus.Queue()
        self.running_task_groups = {}
        self.task_id = count()
        self.async_client = async_client
        self.task_group = async_client.task_group
        self.async_client = async_client

    def close(self):
        self.to_background.sync_q.put((SHUTDOWN, None))

    async def run_cancellable_task(self, coro_or_async_gen):
        context = None
        if inspect.isasyncgen(coro_or_async_gen):
            context = async_null_context(coro_or_async_gen)
        if hasattr(coro_or_async_gen, "__aenter__"):
            context = coro_or_async_gen
        if context:
            async with context as stream:
                try:
                    async for value in stream:
                        await self.from_background.async_q.put((OK, value))
                except anyio.get_cancelled_exc_class():
                    raise
                except Exception as e:
                    await self.from_background.async_q.put((EXCEPTION, e))
            await self.from_background.async_q.put((CLOSE_SENTINEL, None))
        else:
            try:
                success, value = True, await coro_or_async_gen
            except Exception as e:
                success, value = False, e
            await self.from_background.async_q.put((success, value))

    async def manage_cancellable_task(self, task_id, coro_or_async_gen):
        try:
            async with anyio.create_task_group() as self.running_task_groups[task_id]:
                self.running_task_groups[task_id].start_soon(
                    self.run_cancellable_task, coro_or_async_gen
                )
        finally:
            self.running_task_groups.pop(task_id, None)

    async def process_calls_from_foreground_thread(self):
        try:
            while True:
                try:
                    m = await self.to_background.async_q.get()
                except asyncio.CancelledError:
                    self.from_background.async_q.put_nowait(
                        (EXCEPTION, Exception("Connection to server reset."))
                    )
                    break
                code, data = m
                if code is SHUTDOWN:
                    break
                elif code is CANCEL_TASK:
                    maybe_task_group = self.running_task_groups.get(data)
                    if maybe_task_group:
                        maybe_task_group.cancel_scope.cancel()
                elif code is START_TASK:
                    task_id, method, args, kwargs = data
                    coro_or_async_gen = method(self, *args, **kwargs)
                    self.task_group.start_soon(
                        self.manage_cancellable_task, task_id, coro_or_async_gen
                    )
                else:
                    print("received unexpected code", code)
        except:
            traceback.print_exc()
        print("exiting fountainhead background thread")

    @evaluate_context_generator_in_background_thread
    def read_events(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool = False,
    ):
        return self.async_client.read_events(topic, start, end, time_stamps_only)

    @evaluate_in_background_thread
    async def write_event(
        self,
        topic: str,
        event: Any,
        time_stamp: Optional[datetime.fromtimestamp] = None,
        override: bool = False,
    ):
        return await self.async_client.write_event(topic, event, time_stamp, override)


@contextlib.contextmanager
def create_sync_client(host_name: str, port: str, name: str) -> Iterator[Client]:
    client, stop_async = None, None
    ready = threading.Event()

    async def run():
        nonlocal client, stop_async
        stop_async = janus.Queue()
        async with anyio.create_task_group() as task_group:
            async with wrap_websocket_connection(host_name, port) as connection:
                async with _create_async_client(
                    task_group, connection, name
                ) as async_client:
                    client = Client(async_client)
                    task_group.start_soon(client.process_calls_from_foreground_thread)
                    ready.set()
                    await stop_async.async_q.get()
                    task_group.cancel_scope.cancel()

    threading.Thread(target=partial(anyio.run, run), daemon=True).start()
    try:
        ready.wait()
        yield client
    finally:
        stop_async.sync_q.put(None)
