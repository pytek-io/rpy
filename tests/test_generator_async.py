import pytest
from tests.utils_async import scoped_iter, enumerate, sleep

from dyst import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_proxy_object_async,
    RemoteObject,
)


@pytest.mark.anyio
async def test_async_generator():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.count(10)):
            assert i == value


@pytest.mark.anyio
async def test_stream_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async with scoped_iter(proxy.generator_exception(UserException(ERROR_MESSAGE))) as stream:
                async for i, value in enumerate(stream):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE



@pytest.mark.anyio
async def test_stream_early_exit():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with scoped_iter(proxy.count(100)) as numbers:
            async for i in numbers:
                if i == 3:
                    break
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.finally_called
        assert await proxy.current_value == 3
