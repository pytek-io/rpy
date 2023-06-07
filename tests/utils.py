import contextlib
from pickle import dumps, loads
from typing import Any, AsyncIterator, Iterator, List, Tuple

import anyio
import anyio.abc

import dyst.abc
from dyst import AsyncClient, SessionManager, SyncClient, _create_async_client


ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS = 0.1
A_LITTLE_BIT_OF_TIME = 0.1
ERROR_MESSAGE = "an error occured"


class TestConnection(dyst.abc.Connection):
    def __init__(self, sink, stream, name: str) -> None:
        self.name: str = name
        self.sink = sink
        self.stream = stream
        self._closed = anyio.Event()

    def send_nowait(self, message: Tuple[Any, ...]):
        return self.sink.send_nowait(dumps(message))

    async def send(self, message):
        try:
            await self.sink.send(dumps(message))
        except anyio.get_cancelled_exc_class():
            print(f"Sending {message} was cancelled, {self.name} did not send anything.")

    async def __anext__(self) -> Any:
        return loads(await self.stream.receive())

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def aclose(self):
        self.sink.close()
        self.stream.close()
        self._closed.set()

    async def wait_closed(self):
        await self._closed.wait()


def create_test_connection(
    first_name: str, second_name: str
) -> Tuple[TestConnection, TestConnection]:
    first_sink, first_stream = anyio.create_memory_object_stream(100)
    second_sink, second_stream = anyio.create_memory_object_stream(100)
    return TestConnection(first_sink, second_stream, first_name), TestConnection(
        second_sink, first_stream, second_name
    )


@contextlib.asynccontextmanager
async def create_test_async_clients(
    server_object, nb_clients: int = 1
) -> AsyncIterator[List[AsyncClient]]:
    clients = []
    session_manager = SessionManager(server_object)
    async with anyio.create_task_group() as task_group:
        async with contextlib.AsyncExitStack() as exit_stack:
            for i in range(nb_clients):
                client_name = f"client_{i}"
                first, second = create_test_connection(client_name, "server")
                client = await exit_stack.enter_async_context(
                    _create_async_client(task_group, first, name=client_name)
                )
                client_session = await exit_stack.enter_async_context(
                    session_manager.on_new_connection(second)
                )
                task_group.start_soon(client_session.process_messages)
                clients.append(client)
            yield clients
            task_group.cancel_scope.cancel()


@contextlib.contextmanager
def create_test_sync_clients(server_object, nb_clients: int = 1) -> Iterator[List[SyncClient]]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(
            create_test_async_clients(server_object, nb_clients)
        ) as async_clients:
            yield [SyncClient(portal, client) for client in async_clients]


@contextlib.asynccontextmanager
async def create_test_proxy_async_object(
    remote_object_class, server=None, args=()
) -> AsyncIterator[Any]:
    async with create_test_async_clients(server, nb_clients=1) as (client,):
        async with client.create_remote_object(remote_object_class, args, {}) as proxy:
            yield proxy


@contextlib.asynccontextmanager
async def create_proxy_object_async(remote_object, server=None, args=()) -> AsyncIterator[Any]:
    async with create_test_async_clients(remote_object, nb_clients=1) as (client,):
        yield await client.fetch_remote_object(0)


@contextlib.contextmanager
def create_proxy_object_sync(remote_object, server=None, args=()) -> Iterator[Any]:
    with create_test_sync_clients(remote_object, nb_clients=1) as (client,):
        yield client.fetch_remote_object(0)


@contextlib.contextmanager
def create_test_proxy_sync_object(remote_object_class, server=None, args=()) -> Iterator[Any]:
    with create_test_sync_clients(server, nb_clients=1) as (client,):
        with client.create_remote_object(remote_object_class, args, {}) as proxy:
            yield proxy
