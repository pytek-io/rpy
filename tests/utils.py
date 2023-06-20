from __future__ import annotations

import contextlib
from pickle import dumps, loads
from typing import Any, AsyncIterator, Iterator, List, Tuple

import anyio
import anyio.abc
import anyio.lowlevel

import rmy.abc
from rmy import AsyncClient, Server, SyncClient, _create_async_client
from rmy.client_async import ASYNC_GENERATOR_OVERFLOWED_MESSAGE

ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS = 0.1
A_LITTLE_BIT_OF_TIME = 0.1
ERROR_MESSAGE = "an error occured"
TEST_CONNECTION_BUFFER_SIZE = 100


class TestConnection(rmy.abc.Connection):
    def __init__(self, sink, stream, name: str) -> None:
        self.name: str = name
        self.sink = sink
        self.stream = stream
        self._closed = anyio.Event()

    def send_nowait(self, message: Tuple[Any, ...]):
        return self.sink.send_nowait(dumps(message))

    async def drain(self):
        await anyio.lowlevel.checkpoint()

    async def send(self, message):
        try:
            await self.sink.send(dumps(message))
        except anyio.get_cancelled_exc_class():
            print(f"Sending {message} has been cancelled. {self} did not send anything.")
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            print(f"Sending {message} failed, {self.name} did not send anything.")

    async def __anext__(self) -> Any:
        return loads(await self.stream.receive())

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    def close(self):
        self.sink.close()
        self.stream.close()
        self._closed.set()

    async def __aenter__(self) -> TestConnection:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    async def wait_closed(self):
        await self._closed.wait()

    def __str__(self) -> str:
        return f"TestConnection({self.name})"


def create_test_connection(
    first_name: str, second_name: str
) -> Tuple[TestConnection, TestConnection]:
    first_sink, first_stream = anyio.create_memory_object_stream(TEST_CONNECTION_BUFFER_SIZE)
    second_sink, second_stream = anyio.create_memory_object_stream(TEST_CONNECTION_BUFFER_SIZE)
    return TestConnection(first_sink, second_stream, first_name), TestConnection(
        second_sink, first_stream, second_name
    )


@contextlib.asynccontextmanager
async def create_test_async_clients(
    server_object, nb_clients: int = 1
) -> AsyncIterator[List[AsyncClient]]:
    server = Server(server_object)
    async with anyio.create_task_group() as test_task_group:
        async with contextlib.AsyncExitStack() as exit_stack:
            clients = []
            # for i in range(nb_clients):
            i = 0
            client_name = f"client_{i}"
            connection_end_1, connection_end_2 = create_test_connection(client_name, "server")
            client = await exit_stack.enter_async_context(_create_async_client(connection_end_1))
            clients.append(client)
            client_session = await exit_stack.enter_async_context(
                server.on_new_connection(connection_end_2)
            )
            test_task_group.start_soon(client_session.process_messages)
            await exit_stack.enter_async_context(connection_end_1)
            await exit_stack.enter_async_context(connection_end_2)
            yield clients
            test_task_group.cancel_scope.cancel()


@contextlib.contextmanager
def create_test_sync_clients(server_object, nb_clients: int = 1) -> Iterator[List[SyncClient]]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(
            create_test_async_clients(server_object, nb_clients)
        ) as async_clients:
            yield [SyncClient(portal, client) for client in async_clients]


@contextlib.asynccontextmanager
async def create_proxy_object_async(remote_object) -> AsyncIterator[Any]:
    async with create_test_async_clients(remote_object, nb_clients=1) as (client,):
        yield await client.fetch_remote_object(0)


@contextlib.contextmanager
def create_proxy_object_sync(remote_object) -> Iterator[Any]:
    with create_test_sync_clients(remote_object, nb_clients=1) as (client,):
        yield client.fetch_remote_object(0)


@contextlib.contextmanager
def create_test_proxy_object_sync(remote_object_class, server=None, args=()) -> Iterator[Any]:
    with create_test_sync_clients(server, nb_clients=1) as (client,):
        with client.create_remote_object(remote_object_class, args, {}) as proxy:
            yield proxy


class RemoteObject:
    def __init__(self, attribute=None) -> None:
        self.attribute = attribute
        self.ran_tasks = 0
        self.current_value = 0
        self.finally_called = False

    async def echo(self, message: str):
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        return message

    async def throw_exception(self, exception):
        raise exception

    async def sleep_forever(self):
        try:
            await anyio.sleep_forever()
        finally:
            self.ran_tasks += 1

    async def count(self, bound: int) -> AsyncIterator[int]:
        try:
            for i in range(bound):
                await anyio.sleep(A_LITTLE_BIT_OF_TIME)
                self.current_value = i
                yield i
        except GeneratorExit:
            print("Generator exit")
        finally:
            self.finally_called = True

    async def count_nowait(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            yield i

    async def generator_exception(self, exception) -> AsyncIterator[int]:
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise exception
