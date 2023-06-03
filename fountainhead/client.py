import contextlib
import logging
from datetime import datetime
from pickle import loads
from typing import Any, AsyncIterator, Optional, Iterator

import asyncstdlib

from dyst import (
    PubSubManager,
    create_sync_client as create_sync_client_dyst,
    connect,
    remote,
    remote_iter,
)

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
        print("Client created.", server)
        self.server: "Server" = server

    @remote
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

    @remote_iter
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


@contextlib.asynccontextmanager
async def create_async_client(
    host_name: str, port: int, name: str
) -> AsyncIterator[ClientSession]:
    async with connect(host_name, port, name) as client:
        async with client.create_remote_object(ClientSession) as remote_client_session:
            yield remote_client_session


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int, name: str) -> Iterator[ClientSession]:
    with create_sync_client_dyst(host_name, port, name) as client:
        with client.create_remote_object(ClientSession) as remote_client_session:
            yield remote_client_session
