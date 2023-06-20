import contextlib
from typing import Any, Iterator

import anyio

from .client_async import (
    ASYNC_ITERATOR,
    AWAITABLE,
    CLOSE_SENTINEL,
    EXCEPTION,
    FUNCTION,
    OK,
    SERVER_OBJECT_ID,
    AsyncClient,
    connect,
)


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    def sync_generator_iter(self, generator_id):
        with self.portal.wrap_async_context_manager(
            self.async_client.remote_sync_generator_iter(generator_id)
        ) as sync_iterator:
            for code, value in sync_iterator:
                if code == EXCEPTION:
                    raise value
                if code == CLOSE_SENTINEL:
                    break
                yield value

    def wrap_function(self, object_id, function):
        def result(*args, **kwargs):
            code, result = self.portal.call(
                self.async_client.execute_request, FUNCTION, (object_id, function, args, kwargs)
            )
            if code == AWAITABLE:
                return result
            elif code == ASYNC_ITERATOR:
                return self.sync_generator_iter(result)

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
