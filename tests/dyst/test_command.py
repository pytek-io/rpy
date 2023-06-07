import anyio
import anyio.abc
import pytest
from dyst import UserException
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_proxy_object_async,
    create_proxy_object_sync
)


class RemoteObject:
    def __init__(self, attribute=None) -> None:
        self.attribute = attribute
        self.ran_tasks = 0

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


@pytest.mark.anyio
async def test_connection():
    async with create_proxy_object_async(RemoteObject(None)) as proxy:
        assert await proxy.echo("test") == "test"


@pytest.mark.anyio
async def test_attribute():
    value = "test"
    async with create_proxy_object_async(RemoteObject(value)) as proxy:
        returned_value = await proxy.attribute
        assert returned_value is not value
        assert returned_value == value


def test_attribute_sync():
    value = "test"
    with create_proxy_object_sync(RemoteObject(value)) as proxy:
        returned_value = proxy.attribute
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_non_existent_attribute():
    async with create_proxy_object_async(RemoteObject("test",)) as proxy:
        with pytest.raises(AttributeError):
            await proxy.dummy


@pytest.mark.anyio
async def test_coroutine():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo(value)
        assert returned_value is not value
        assert returned_value == value


def test_coroutine_sync():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo(value)
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_coroutine_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(UserException) as e_info:
            await proxy.throw_exception(UserException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
async def test_coroutine_cancellation():
    async with create_proxy_object_async(RemoteObject()) as proxy:
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
async def test_coroutine_time_out():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever()
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.ran_tasks == 1
