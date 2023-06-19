import contextlib
from typing import Iterator

import anyio
import janus

from .client_async import (
    ASYNC_ITERATOR,
    AWAITABLE,
    CLOSE_STREAM,
    EXCEPTION,
    FUNCTION,
    OK,
    SERVER_OBJECT_ID,
    AsyncClient,
    connect,
)
from .common import cancel_task_on_exit
import asyncstdlib


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    @contextlib.asynccontextmanager
    async def remote_async_iterate(self, iterator_id):
        queue = janus.Queue(3)

        async def forward_to_main_thread():
            try:
                async with asyncstdlib.scoped_iter(
                    self.async_client.iter_async_generator(iterator_id)
                ) as stream:
                    async for value in stream:
                        queue.async_q.put_nowait((OK, value))
                        if queue.async_q.unfinished_tasks == queue.async_q.maxsize - 1:
                            queue.async_q.put_nowait((Exception, "Queue full"))
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                queue.async_q.put_nowait((EXCEPTION, e))
            finally:
                queue.async_q.put_nowait((CLOSE_STREAM, None))

        def result_iterator():
            while True:
                code, message = queue.sync_q.get()
                queue.sync_q.task_done()
                if code in CLOSE_STREAM:
                    queue.close()
                    break
                if code is EXCEPTION:
                    queue.close()
                    raise message
                yield message

        with cancel_task_on_exit(forward_to_main_thread()):
            yield result_iterator()

    def sync_generator(self, iterator_id: int):
        with self.portal.wrap_async_context_manager(
            self.remote_async_iterate(iterator_id)
        ) as sync_iterator:
            yield from sync_iterator

    def wrap_function(self, object_id, function):
        def result(*args, **kwargs):
            code, result = self.portal.call(
                self.async_client.execute_request, FUNCTION, (object_id, function, args, kwargs)
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

    def fetch_remote_object(self, object_id: int = SERVER_OBJECT_ID):
        return self.portal.call(self.async_client.fetch_remote_object, object_id, self)


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect(host_name, port)) as async_client:
            yield SyncClient(portal, async_client)
