import contextlib
from datetime import datetime
from typing import Any, Iterator, Optional

import anyio
import janus

from dyst import CLOSE_STREAM, EXCEPTION, OK
from .client_async import AsyncClient, connect


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    @contextlib.asynccontextmanager
    async def _wrap_context_async_stream(self, cancellable_stream, *args, **kwargs):
        queue = janus.Queue()

        async def wrapper():
            try:
                async with cancellable_stream as result_stream:
                    async for value in result_stream:
                        await queue.async_q.put((OK, value))
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                await queue.async_q.put((EXCEPTION, e))
            finally:
                await queue.async_q.put((CLOSE_STREAM, None))
                task_group.cancel_scope.cancel()

        def result_sync_iterator():
            while True:
                code, message = queue.sync_q.get()
                if code in CLOSE_STREAM:
                    queue.close()
                    break
                if code is EXCEPTION:
                    queue.close()
                    raise message
                yield message

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(wrapper)
            yield result_sync_iterator()

    def wrap_async_context_stream(self, cancellable_stream):
        return self.portal.wrap_async_context_manager(
            self._wrap_context_async_stream(cancellable_stream)
        )

    def wrap_awaitable(self, method, *args, **kwargs):
        def result(*args, **kwargs):
            return self.portal.call(method, *args, **kwargs)

        return result

    def create_remote_object(self, object_class, args=(), kwarg={}):
        return self.portal.wrap_async_context_manager(
            self.async_client.create_remote_object(object_class, args, kwarg, sync_client=self)
        )


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int, name: str) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect(host_name, port, name)) as async_client:
            yield SyncClient(portal, async_client)


