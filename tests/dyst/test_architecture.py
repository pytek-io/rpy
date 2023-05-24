from functools import partial, wraps
from typing import AsyncIterator

import anyio
import asyncstdlib
import pytest
import asyncio.streams
from dyst import AsyncClientCore, ClientSessionBase, ServerBase, UserException
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    create_test_environment,
)


FINALLY_CALLED = "finally called"
ERROR_MESSAGE = "an error occured"


def update_running_tasks(method):
    @wraps(method)
    async def result(self: "ClientSession", *args, **kwargs):
        self.running_tasks += 1
        try:
            return await method(self, *args, **kwargs)
        finally:
            self.running_tasks -= 1
            self.ran_tasks += 1

    return result


class ClientSession:
    def __init__(self, server, session_core: ClientSessionBase) -> None:
        self.session_core = session_core
        self.server = server
        self.running_tasks = 0
        self.ran_tasks = 0

    async def echo(self, message: str):
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        return message

    async def throw_exception(self, exception):
        raise exception

    @update_running_tasks
    async def sleep_forever(self):
        await anyio.sleep_forever()

    @update_running_tasks
    async def count(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i

    async def stream_exception(self, bound: int):
        for i in range(bound):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise UserException(ERROR_MESSAGE)

    @update_running_tasks
    async def buggy_cancellable_method(self):
        try:
            await anyio.sleep_forever()
        except Exception:
            with anyio.CancelScope(shield=True):
                print("hello" * 20)
                await anyio.sleep_forever()
                print("hello" * 20)


class AsyncClient:
    def __init__(self, client: AsyncClientCore) -> None:
        self.client = client

    async def echo(self, message: str):
        return await self.client.evaluate_command(ClientSession.echo, (message,))

    def count(self, bound: int):
        return self.client.subscribe_stream(ClientSession.count, (bound,))

    def stream_exception(self, bound: int):
        return self.client.subscribe_stream(ClientSession.stream_exception, (bound,))


@pytest.mark.anyio
async def test_successfull_evaluation():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClient = environ.clients[0]
        value = "test"
        returned_value = await client.echo(value)
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_command_exception():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClientCore = environ.clients[0].client
        with pytest.raises(UserException) as e_info:
            await client.evaluate_command(
                ClientSession.throw_exception, (UserException(ERROR_MESSAGE),)
            )
        assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
async def test_command_cancellation():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClientCore = environ.clients[0].client
        client_session = environ.server.sessions[client.name]
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await client.evaluate_command(ClientSession.sleep_forever, ())
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert client_session.running_tasks == 0 and client_session.ran_tasks == 1


@pytest.mark.anyio
async def test_command_buggy_cancellation():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClientCore = environ.clients[0].client
        client_session = environ.server.sessions[client.name]
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await client.evaluate_command(ClientSession.buggy_cancellable_method, ())
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert client_session.running_tasks == 0 and client_session.ran_tasks == 1


@pytest.mark.anyio
async def test_stream_cancellation():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClientCore = environ.clients[0].client
        client_session = environ.server.sessions[client.name]
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                async with client.create_cancellable_stream(
                    ClientSession.count, (100,)
                ) as numbers:
                    async for i in numbers:
                        print(i)
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert client_session.running_tasks == 0 and client_session.ran_tasks == 1


@pytest.mark.anyio
async def test_simple():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClient = environ.clients[0]
        async with client.count(10) as events:
            async for i, value in asyncstdlib.enumerate(events):
                assert i == value
        with pytest.raises(Exception) as e_info:
            async with client.stream_exception(10) as events:
                async for i, value in asyncstdlib.enumerate(events):
                    print(i, value)
        print(e_info.value.args[0])
