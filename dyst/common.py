import contextlib
import traceback
from typing import Any, AsyncIterator, Callable

import anyio
import asyncstdlib


async def forward_stream(stream, sink):
    async with asyncstdlib.closing(sink):
        async for value in stream:
            await sink.send(value)


@contextlib.asynccontextmanager
async def create_context_async_generator(cancellable_stream: Callable) -> AsyncIterator[Any]:
    """Return an async context manager that will cancel the cancellable stream on exit.
    A cancellable stream is similar to an async generator but for the fact that it returns
    its values via a sink callback. It will be notified upon termination
    through the standard anyio cancellation mechanism, allowing it to take any clean up action.
    We cannot support async generators directly because those won't be
    notified of any exception if they are in a yield statement when the cancellation occurs.
    """
    result_sink, result_stream = anyio.create_memory_object_stream()

    async def async_generator():
        with contextlib.suppress(anyio.EndOfStream):
            while True:
                yield await result_stream.receive()

    async def wrapper():
        async with asyncstdlib.closing(result_sink):
            await cancellable_stream(result_sink.send)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(
            wrapper,
            name=f"{cancellable_stream} evaluation",
        )
        yield async_generator()
        task_group.cancel_scope.cancel()


def identity(x):
    return x


@contextlib.contextmanager
def print_error_stack(location):
    try:
        yield
    except Exception:
        traceback.print_exc()
        print(location)
        print("=" * 20)
        raise
