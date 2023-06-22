import pytest

from rmy import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
    RemoteObject,
    create_proxy_object_async,
)
from tests.utils_async import enumerate, scoped_iter, sleep

pytestmark = pytest.mark.anyio


async def test_simple_iteration():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.count(10)):
            assert i == value


async def test_stream_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async with scoped_iter(
                proxy.async_generator_exception(UserException(ERROR_MESSAGE))
            ) as stream:
                async for i, value in enumerate(stream):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE


async def test_stream_early_exit():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with scoped_iter(proxy.count(100)) as numbers:
            async for i in numbers:
                if i == 3:
                    break
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert await proxy.finally_called
        assert await proxy.current_value == 3


async def test_overflow():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async for i in proxy.count_nowait(120):
                await sleep(0.1)
        assert e_info.value.args[0] == ASYNC_GENERATOR_OVERFLOWED_MESSAGE
