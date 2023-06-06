import contextlib
from typing import Iterator

import anyio
import janus

from dyst import CLOSE_STREAM, EXCEPTION, OK

from .client_async import ASYNC_ITERATOR, AWAITABLE, FUNCTION, AsyncClient, connect


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    @contextlib.asynccontextmanager
    async def remote_async_iterate(self, iterator_id):
        queue = janus.Queue()

        async def forwarding_task():
            try:
                async for value in self.async_client.iter_async_generator(iterator_id):
                    await queue.async_q.put((OK, value))
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                await queue.async_q.put((EXCEPTION, e))
            finally:
                await queue.async_q.put((CLOSE_STREAM, None))
                if not task_group.cancel_scope.cancel_called:
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
            task_group.start_soon(forwarding_task)
            yield result_sync_iterator()

    def sync_generator(self, iterator_id: int):
        with self.portal.wrap_async_context_manager(
            self.remote_async_iterate(iterator_id)
        ) as sync_iterator:
            yield from sync_iterator

    def wrap_function(self, object_id, function):
        def result(*args, **kwargs):
            code, result = self.portal.call(
                self.async_client.manage_request, FUNCTION, (object_id, function, args, kwargs)
            )
            if code == AWAITABLE:
                return result
            elif code == ASYNC_ITERATOR:
                return self.sync_generator(result)

        return result

    def create_remote_object(self, object_class, args=(), kwarg={}):
        return self.portal.wrap_async_context_manager(
            self.async_client.create_remote_object(object_class, args, kwarg, sync_client=self)
        )

    def wrap_awaitable(self, method):
        def result(_self, *args, **kwargs):
            return self.portal.call(method, *args, **kwargs)

        return result


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int, name: str) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect(host_name, port, name)) as async_client:
            yield SyncClient(portal, async_client)
