import contextlib
from datetime import datetime
from pickle import dumps, loads  # could be any arbitrary serialization method
from typing import Any, AsyncIterable, Optional, Iterator

import anyio

from dyst import AsyncClientCore, _create_async_client_core, wrap_websocket_connection, SyncClientBase, identity

from .server import ClientSession


OK = "OK"
START_TASK = "Start task"
CLOSE_SENTINEL = "Done"
SHUTDOWN = "Shutdown"
CLOSE_STREAM = "Close stream"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"

STREAM_BUFFER = 100


def load_time_stamp_and_value(time_stamp_and_value):
    time_stamp, value = time_stamp_and_value
    return time_stamp, loads(value)


class AsyncClient:
    def __init__(self, client: AsyncClientCore, serializer=None, deserializer=None) -> None:
        self.client = client
        self.serializer = serializer or dumps
        self.deserializer = deserializer or loads

    def read_events(
        self,
        topic: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        time_stamps_only: bool = False,
    ):
        return self.client.subscribe_stream(
            ClientSession.read_events,
            (topic, start, end, time_stamps_only),
            identity if time_stamps_only else load_time_stamp_and_value,
        )

    async def write_event(
        self,
        topic: str,
        event: Any,
        time_stamp: Optional[datetime] = None,
        override: bool = False,
    ) -> datetime:
        return await self.client.send_command(
            ClientSession.write_event,
            (topic, self.serializer(event), time_stamp, override),
        )

    async def read_event(self, topic: str, time_stamp: datetime):
        return self.deserializer(
            await self.client.send_command(ClientSession.read_event, (topic, time_stamp))
        )


@contextlib.asynccontextmanager
async def create_async_client(host_name: str, port: int, name: str) -> AsyncIterable[AsyncClient]:
    async with anyio.create_task_group() as task_group:
        async with wrap_websocket_connection(host_name, port) as connection:
            async with _create_async_client_core(task_group, connection, name) as client:
                yield AsyncClient(client)

class SyncClient(SyncClientBase):
    def read_events(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool = False,
    ):
        return self.wrap_async_context_stream(
            self.async_client.read_events(topic, start, end, time_stamps_only)
        )

    def write_event(
        self,
        topic: str,
        event: Any,
        time_stamp: Optional[datetime] = None,
        override: bool = False,
    ):
        return self.wrap_awaitable(
            self.async_client.write_event(topic, event, time_stamp, override)
        )


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int, name: str) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(
            create_async_client(host_name, port, name)
        ) as async_client:
            yield SyncClient(portal, async_client)
