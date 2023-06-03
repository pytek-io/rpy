import anyio
import anyio.abc
import pytest

from dyst import UserException, remote
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_test_proxy_async_object,
)


class RemoteObject:
    def __init__(self, server, attribute=None) -> None:
        self.server = server
        self.ran_tasks = 0
        self.attribute = attribute

    @remote
    async def echo(self, message: str):
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        return message

    @remote
    async def throw_exception(self, exception):
        raise exception

    @remote
    async def sleep_forever(self):
        try:
            await anyio.sleep_forever()
        finally:
            self.ran_tasks += 1


@pytest.mark.anyio
async def test_attribute():
    async with create_test_proxy_async_object(RemoteObject, args=("test",)) as proxy:
        assert "test" == await proxy.attribute


@pytest.mark.anyio
async def test_command_echo():
    async with create_test_proxy_async_object(RemoteObject) as proxy:
        value = "test"
        returned_value = await proxy.echo(value)
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_command_exception():
    async with create_test_proxy_async_object(RemoteObject) as proxy:
        with pytest.raises(UserException) as e_info:
            await proxy.throw_exception(UserException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
async def test_command_cancellation():
    async with create_test_proxy_async_object(RemoteObject) as proxy:
        async with anyio.create_task_group() as task_group:

            async def cancellable_task(task_status: anyio.abc.TaskStatus):
                task_status.started()
                await proxy.sleep_forever()

            await task_group.start(cancellable_task)
            await anyio.sleep(0.1)
            task_group.cancel_scope.cancel()
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.ran_tasks == 1


@pytest.mark.anyio
async def test_command_time_out():
    async with create_test_proxy_async_object(RemoteObject) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever()
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.ran_tasks == 1
