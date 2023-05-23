import contextlib

import anyio
import pytest
from asyncstdlib import enumerate
from dyst import create_context_async_generator


FINAL_VALUE = "final value"
ERROR_MESSAGE = "an error occured"


def create_cancellable_stream(flag, bound):
    async def cancellable_stream(sink):
        i = 0
        try:
            for i in range(bound):
                await sink(i)
                await anyio.sleep(0)
        finally:
            # rmk: any async clean up action has to be shielded otherwise it will not run (ie: thrown cancel exception)
            with anyio.CancelScope(shield=True):
                await anyio.sleep(0)
                flag[FINAL_VALUE] = i

    return cancellable_stream


@contextlib.asynccontextmanager
async def create_cancellable_stream_(nb_iterations, expected_stop_value: int):
    flag = {FINAL_VALUE: None}
    context_async_generator = create_context_async_generator(
        create_cancellable_stream(flag, nb_iterations)
    )

    async def result():
        async for index, value in enumerate(value_stream):
            assert index == value
            yield value

    async with context_async_generator as value_stream:
        yield result()
    assert flag[FINAL_VALUE] == expected_stop_value


@pytest.mark.anyio
async def test_normal_execution():
    bound = 5
    async with create_cancellable_stream_(bound, bound - 1) as value_stream:
        async for value in value_stream:
            pass


@pytest.mark.anyio
async def test_early_exit():
    bound, stop = 5, 2
    async with create_cancellable_stream_(bound, stop + 1) as value_stream:
        async for value in value_stream:
            if value == stop:
                break


@pytest.mark.anyio
async def test_exception():
    bound, stop = 5, 2
    with pytest.raises(Exception) as e_info:
        async with create_cancellable_stream_(bound, stop + 1) as value_stream:
            async for value in value_stream:
                if value == stop:
                    raise Exception(ERROR_MESSAGE)
    assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
async def test_cancellation():
    bound, stop = 5, 2

    async with anyio.create_task_group() as task_group:

        async def cancellable_task():
            async with create_cancellable_stream_(bound, stop + 1) as value_stream:
                async for value in value_stream:
                    if value == stop:
                        task_group.cancel_scope.cancel()

        task_group.start_soon(cancellable_task)
