import anyio
import pytest
from functools import partial
from fountainhead.common import create_context_async_generator


FINALLY_CALLED = "finally called"
ERROR_MESSAGE = "an error occured"


async def cancellable_stream(flag, sink):
    try:
        for i in range(10):
            await sink(i)
            await anyio.sleep(0)
    finally:
        # rmk: any async clean up action has to be shielded otherwise it will not run (ie: thrown cancel exception)
        with anyio.CancelScope(shield=True):
            await anyio.sleep(0)
            flag[FINALLY_CALLED] = True


@pytest.mark.anyio
@pytest.mark.parametrize("stop", [2, 11])
async def test_cancellable_async_generator(stop):
    flag = {FINALLY_CALLED: False}
    context_async_generator = create_context_async_generator(partial(cancellable_stream, flag))
    async with context_async_generator as value_stream:
        async for value in value_stream:
            if value == stop:
                break
    assert flag[FINALLY_CALLED]
