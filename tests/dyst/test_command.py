import contextlib
from functools import partial, wraps
from typing import AsyncIterator, Tuple, List

import anyio
import anyio.abc
import pytest

from dyst import ServerBase, UserException
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_test_environment_core,
)


FINALLY_CALLED = "finally called"


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
    def __init__(self, server, name: str) -> None:
        self.server = server
        self.name: str = name
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

    async def count(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i

    async def stream_exception(self, exception):
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise exception


@contextlib.asynccontextmanager
async def create_test_environment():
    async with create_test_environment_core(partial(ServerBase, ClientSession)) as (
        server,
        clients,
    ):
        yield server, clients


@contextlib.asynccontextmanager
async def create_test_environment_new() -> AsyncIterator[Tuple[ServerBase, List[ClientSession]]]:
    async with create_test_environment_core(partial(ServerBase, ClientSession)) as (
        server,
        clients,
    ):
        yield server, [ClientSession(server, name="client_0") for client in clients]


@pytest.mark.anyio
async def test_command_echo():
    async with create_test_environment() as (_server, (client,)):
        value = "test"
        returned_value = await client.evaluate_command(ClientSession.echo, (value,))
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_command_exception():
    async with create_test_environment() as (_server, (client,)):
        with pytest.raises(UserException) as e_info:
            await client.evaluate_command(
                ClientSession.throw_exception, (UserException(ERROR_MESSAGE),)
            )
        assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
async def test_command_cancellation():
    async with create_test_environment() as (server, (client,)):
        async with anyio.create_task_group() as task_group:

            async def cancellable_task(task_status: anyio.abc.TaskStatus):
                task_status.started()
                await client.evaluate_command(ClientSession.sleep_forever, ())

            await task_group.start(cancellable_task)
            await anyio.sleep(0.1)
            task_group.cancel_scope.cancel()
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        client_session = server.sessions[client.name]
        assert client_session.running_tasks == 0 and client_session.ran_tasks == 1


@pytest.mark.anyio
async def test_command_time_out():
    async with create_test_environment() as (server, (client,)):
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await client.evaluate_command(ClientSession.sleep_forever, ())
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        client_session = server.sessions[client.name]
        assert client_session.running_tasks == 0 and client_session.ran_tasks == 1
