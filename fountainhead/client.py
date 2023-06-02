import logging
from datetime import datetime
from pickle import loads
from typing import Any, AsyncIterator, Optional

import asyncstdlib

from dyst import PubSubManager

from .storage import Storage


class Server:
    def __init__(self, event_folder) -> None:
        self.storage = Storage(event_folder)
        self.pub_sub_manager = PubSubManager()


def identity(x):
    return x


def load_time_stamp_and_value(time_stamp_and_value):
    time_stamp, value = time_stamp_and_value
    return time_stamp, loads(value)


class ClientSession:
    def __init__(self, server: "Server") -> None:
        self.server: "Server" = server

    async def write_event(
        self,
        topic: str,
        event: bytes,
        time_stamp: Optional[datetime] = None,
        overwrite: bool = False,
    ):
        time_stamp = time_stamp or datetime.now()
        await self.server.storage.write(topic, str(time_stamp.timestamp()), event, overwrite)
        logging.info(f"Saved event from {self} under: {topic}{time_stamp}")
        self.server.pub_sub_manager.broadcast_to_subscriptions(topic, time_stamp)
        return time_stamp

    async def read_event(self, topic: str, time_stamp: datetime):
        return await self.server.storage.read(topic, str(time_stamp.timestamp()))

    async def read_events(
        self,
        topic: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        time_stamps_only: bool = False,
    ) -> AsyncIterator[Any]:
        with self.server.pub_sub_manager.subscribe(topic) as subscription:
            existing_tags = self.server.storage.list_topic(topic, start, end)
            async for time_stamp in asyncstdlib.chain(existing_tags, subscription):
                if end and datetime.now() > end:
                    break
                yield (
                    time_stamp
                    if time_stamps_only
                    else (
                        time_stamp,
                        await self.read_event(topic, time_stamp),
                    )
                )


# @contextlib.asynccontextmanager
# async def create_async_client(host_name: str, port: int, name: str) -> AsyncIterator[Client]:
#     async with anyio.create_task_group() as task_group:
#         async with connect(host_name, port) as connection:
#             async with _create_async_client_core(task_group, connection, name) as client:
#                 yield ClientSession(client)


# class SyncClient(SyncClientBase):
#     def read_events(
#         self,
#         topic: str,
#         start: Optional[datetime],
#         end: Optional[datetime],
#         time_stamps_only: bool = False,
#     ):
#         return self.wrap_async_context_stream(
#             self.async_client.read_events(topic, start, end, time_stamps_only)
#         )

#     def write_event(
#         self,
#         topic: str,
#         event: Any,
#         time_stamp: Optional[datetime] = None,
#         override: bool = False,
#     ):
#         return self.wrap_awaitable(
#             self.async_client.write_event(topic, event, time_stamp, override)
#         )


# @contextlib.contextmanager
# def create_sync_client(host_name: str, port: int, name: str) -> Iterator[SyncClient]:
#     with anyio.start_blocking_portal("asyncio") as portal:
#         with portal.wrap_async_context_manager(
#             create_async_client(host_name, port, name)
#         ) as async_client:
#             yield SyncClient(portal, async_client)
