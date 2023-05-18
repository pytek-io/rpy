import anyio
import pytest

from fountainhead.common import create_context_async_generator

FINALLY_CALLED = "finally called"

async def cancellable_stream_example(sink, flag):
    try:
        for i in range(10):
            await sink(i)
            await anyio.sleep(0.1)
    finally:
        # rmk: we need this to allow async methods to run inside this scope
        with anyio.CancelScope(shield=True) as scope:
            await anyio.sleep(0)
            flag[FINALLY_CALLED] = True


@pytest.mark.anyio
async def test_cancellable_async_generator():
    async def test(stop):
        flag = {FINALLY_CALLED: False}
        context_async_generator = create_context_async_generator(
            cancellable_stream_example, flag
        )
        async with context_async_generator as value_stream:
            async for value in value_stream:
                if value == stop:
                    break
        assert flag[FINALLY_CALLED]

    # checking we executed the final clause when the caller stop the iteration or the iteration stops by itself.
    await test(stop=2)
    await test(stop=100)
