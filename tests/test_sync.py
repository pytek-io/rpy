import contextlib
import anyio
from fountainhead.client_sync import SyncClientBase
from typing import Iterable


class AsyncClient:
    async def add_numbers(self, a, b):
        await anyio.sleep(0.1)
        return a + b

    async def async_stream(self, result_sink, bound):
        for i in range(bound):
            print(i)
            await result_sink(i)


class SyncClient(SyncClientBase):
    def add_numbers(self, a, b):
        return self.wrap_async_call(self.async_client.add_numbers, a, b)

    def sync_stream(self, bound):
        return self.wrap_async_stream(
            self.async_client.async_stream, bound
        )


@contextlib.asynccontextmanager
async def create_async_client():
    yield AsyncClient()


@contextlib.contextmanager
def create_sync_client() -> Iterable[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(create_async_client()) as async_client:
            yield SyncClient(portal, async_client)


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
