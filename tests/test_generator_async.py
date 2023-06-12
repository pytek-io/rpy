import pytest

from rmy import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    RemoteObject,
    create_proxy_object_async,
)
from tests.utils_async import enumerate, scoped_iter, sleep, sleep_forever


@pytest.mark.anyio
async def test_async_generator():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.count(10)):
            assert i == value


@pytest.mark.anyio
async def test_stream_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async with scoped_iter(
                proxy.generator_exception(UserException(ERROR_MESSAGE))
            ) as stream:
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
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert await proxy.finally_called
        assert await proxy.current_value == 3

import asyncio
async def print_tasks():
    for task in asyncio.all_tasks():
        task.print_stack()

@pytest.mark.anyio
async def test_slow_consumer():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async for i in proxy.count_nowait(1000):
                await sleep(1)
                # for task in asyncio.all_tasks():
                #     task.print_stack()
