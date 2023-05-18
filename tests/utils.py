import anyio
from pickle import dumps, loads
from typing import Any, AsyncIterable


class TestConnection:
    def __init__(self, sink, stream) -> None:
        self.sink = sink
        self.stream = stream
        self._closed = anyio.Event()

    async def send(self, message):
        try:
            await self.sink.send(dumps(message))
        except anyio.get_cancelled_exc_class():
            print("failed to send", message)

    async def recv(self):
        return loads(await self.stream.receive())

    async def __aiter__(self) -> AsyncIterable[Any]:
        async for message in self.stream:
            yield loads(message)

    async def close(self):
        self.sink.close()
        self.stream.close()

    async def wait_closed(self):
        await self._closed.wait()


def create_test_connection():
    first_sink, first_stream = anyio.create_memory_object_stream(100)
    second_sink, second_stream = anyio.create_memory_object_stream(100)
    return TestConnection(first_sink, second_stream), TestConnection(
        second_sink, first_stream
    )
