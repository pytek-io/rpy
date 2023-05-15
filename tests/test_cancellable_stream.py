import anyio
import pytest

from fountainhead.common import create_context_async_generator


async def cancellable_stream_example(sink, flag):
    try:
        for i in range(10):
            await sink(i)
            await anyio.sleep(0.1)
    finally:
        flag["value"] = True


@pytest.mark.anyio
async def test_cancellable_async_generator():
    async def test(stop):
        flag = {"value": False}
        context_async_generator = create_context_async_generator(
            cancellable_stream_example, flag
        )
        async with context_async_generator as value_stream:
            async for value in value_stream:
                if value == stop:
                    break
        assert flag["value"]

    # checking we executed the final clause when the caller stop the iteration or the iteration stops by itself.
    await test(stop=2)
    await test(stop=100)
