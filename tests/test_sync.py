import contextlib
from typing import AsyncIterable, Iterable

import anyio
import pytest

from dyst import create_context_async_generator, SyncClientBase


class AsyncClient:
    async def add_numbers(self, a, b):
        await anyio.sleep(0)
        return a + b

    def async_stream(self, bound):
        async def cancellable_stream(sink):
            for i in range(bound):
                await sink(i)

        return create_context_async_generator(cancellable_stream)


class SyncClient(SyncClientBase):
    def add_numbers(self, a, b):
        return self.wrap_awaitable(self.async_client.add_numbers(a, b))

    def sync_stream(self, bound):
        return self.wrap_async_context_stream(self.async_client.async_stream(bound))


@contextlib.asynccontextmanager
async def create_async_client() -> AsyncIterable[AsyncClient]:
    yield AsyncClient()


@contextlib.contextmanager
def create_sync_client() -> Iterable[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(create_async_client()) as async_client:
            yield SyncClient(portal, async_client)


@pytest.mark.anyio
async def test_async():
    async_client = AsyncClient()
    async with async_client.async_stream(10) as events:
        async for event in events:
            print(event)


def test_sync():
    with create_sync_client() as client:
        assert client.add_numbers(1, 2) == 3

        with client.sync_stream(5) as s:
            for i, m in enumerate(s):
                assert i == m

        with client.sync_stream(5) as s:
            for i, m in enumerate(s):
                if i == 2:
                    return
