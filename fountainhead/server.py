import contextlib
import logging
from datetime import datetime
from pickle import loads
from typing import Any, AsyncIterator, Optional, Iterator

import asyncstdlib

import rpy
from .storage import Storage


def load_time_stamp_and_value(time_stamp_and_value):
    time_stamp, value = time_stamp_and_value
    return time_stamp, loads(value)


class Server:
    def __init__(self, event_folder) -> None:
        self.storage = Storage(event_folder)
        self.pub_sub_manager = rpy.PubSubManager()

    async def write_event(
        self,
        topic: str,
        event: bytes,
        time_stamp: Optional[datetime] = None,
        overwrite: bool = False,
    ):
        time_stamp = time_stamp or datetime.now()
        await self.storage.write(topic, str(time_stamp.timestamp()), event, overwrite)
        logging.info(f"Saved event from {self} under: {topic}{time_stamp}")
        self.pub_sub_manager.broadcast_to_subscriptions(topic, time_stamp)
        return time_stamp

    async def read_event(self, topic: str, time_stamp: datetime):
        return await self.storage.read(topic, str(time_stamp.timestamp()))

    async def read_events(
        self,
        topic: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        time_stamps_only: bool = False,
    ) -> AsyncIterator[Any]:
        udpates = self.pub_sub_manager.subscribe(topic)
        existing_tags = self.storage.list_topic(topic, start, end)
        async for time_stamp in asyncstdlib.chain(existing_tags, udpates):
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


@contextlib.asynccontextmanager
async def create_async_client(host_name: str, port: int) -> AsyncIterator[Server]:
    async with rpy.connect(host_name, port) as client:
        yield await client.fetch_remote_object()


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[Server]:
    with rpy.create_sync_client(host_name, port) as client:
        yield client.fetch_remote_object()
