import contextlib
import anyio
from typing import Any, AsyncIterable, AsyncContextManager, Callable


async def forward_stream(stream, sink):
    async for value in stream:
        await sink.send(value)
    await sink.aclose()


async def convert_to_async_generator(stream):
    try:
        while True:
            yield await stream()
    except anyio.EndOfStream:
        pass


@contextlib.asynccontextmanager
async def async_null_context(value):
    """same as contextlib.nullcontext, but async..."""
    yield value


@contextlib.asynccontextmanager
async def create_context_async_generator(
    cancellable_stream: Callable, *args
) -> AsyncContextManager[AsyncIterable[Any]]:
    """Return an async context manager that will cancel the cancellable stream on exit.
       A cancellable stream is similar to an async generator but for the fact that it returns
       its values via a sink callback. It will be notified upon termination 
       through the standard anyio cancellation mechanism, allowing it to take any clean up action. 
       We cannot support async generators directly because those won't be 
       notified of any Exception if they are in a yield statement when the cancellation occurs. 
    """
    result_sink, result_stream = anyio.create_memory_object_stream()

    async def wrapper():
        try:
            await cancellable_stream(result_sink.send, *args)
        finally:
            await result_sink.aclose()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(wrapper)
        yield convert_to_async_generator(result_stream.receive)
        task_group.cancel_scope.cancel()


