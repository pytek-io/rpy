import pytest
import anyio
import anyio.abc
from tests.utils_async import scoped_iter, sleep

from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    create_proxy_object_async,
    RemoteObject,
)


@pytest.mark.anyio
async def test_stream_cancellation():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                async with scoped_iter(proxy.count(100)) as numbers:
                    async for i in numbers:
                        pass
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.finally_called


@pytest.mark.anyio
async def test_coroutine_cancellation():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group() as task_group:

            async def cancellable_task(task_status: anyio.abc.TaskStatus):
                task_status.started()
                await proxy.sleep_forever()

            await task_group.start(cancellable_task)
            await sleep(0.1)
            task_group.cancel_scope.cancel()
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.ran_tasks == 1


@pytest.mark.anyio
async def test_coroutine_time_out():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever()
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.ran_tasks == 1
