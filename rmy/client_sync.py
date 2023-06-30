import contextlib
import itertools
from typing import Iterator

import anyio

from .client_async import (
    ASYNC_GENERATOR,
    SYNC_GENERATOR,
    COROUTINE,
    METHOD,
    MOVE_ASYNC_ITERATOR,
    OK,
    SERVER_OBJECT_ID,
    VALUE,
    AsyncClient,
    connect,
    decode_iteration_result,
)


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    def _sync_generator_iter(self, push_or_pull, generator_id):
        with self.portal.wrap_async_context_manager(
            self.async_client._remote_sync_generator_iter(generator_id)
        ) as sync_iterator:
            for index, (terminated, value) in enumerate(
                itertools.starmap(decode_iteration_result, sync_iterator)
            ):
                if terminated:
                    break
                yield value
                if not push_or_pull:
                    self.portal.call(
                        self.async_client._send, MOVE_ASYNC_ITERATOR, generator_id, (index + 1,)
                    )

    def _wrap_function(self, object_id, function):
        def result(*args, **kwargs):
            code, result = self.portal.call(
                self.async_client.execute_request, METHOD, (object_id, function, args, kwargs)
            )
            if code in (OK, VALUE):
                return result
            elif code in (ASYNC_GENERATOR, SYNC_GENERATOR):
                return self._sync_generator_iter(code == ASYNC_GENERATOR, result)
            elif code == COROUTINE:
                return self.portal.call(
                    self.async_client.execute_request, COROUTINE, (result,), False, False
                )
            else:
                raise Exception(f"Unexpected code: {code}")

        return result

    def _wrap_awaitable(self, method):
        def result(_self, *args, **kwargs):
            return self.portal.call(method, *args, **kwargs)

        return result

    def fetch_remote_object(self, object_id: int = SERVER_OBJECT_ID):
        return self.portal.call(self.async_client._fetch_remote_object, object_id, self)

    def create_remote_object(self, object_class, args=(), kwarg={}):
        return self.portal.wrap_async_context_manager(
            self.async_client.create_remote_object(object_class, args, kwarg, sync_client=self)
        )


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect(host_name, port)) as async_client:
            yield SyncClient(portal, async_client)
