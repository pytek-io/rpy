import logging
import os
from datetime import datetime
from typing import Any, Optional

import anyio
import asyncstdlib

from dyst import ServerBase, UserException


overwrite_ERROR_MESSAGE = "Trying to overwrite an existing event without overwrite set to True."


class Storage:
    def __init__(self, base_folder: str) -> None:
        self.base_folder = base_folder

    async def write(self, topic: str, name: str, content: Any, overwrite: bool):
        if topic.startswith("/"):
            topic = topic[1:]
        folder_path = os.path.join(self.base_folder, topic)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, name)
        if os.path.exists(file_path) and not overwrite:
            raise UserException(overwrite_ERROR_MESSAGE)
        async with await anyio.open_file(file_path, "wb") as file:
            await file.write(content)

    async def read(self, topic: str, name: str):
        file_path = os.path.join(self.base_folder, topic, name)
        async with await anyio.open_file(file_path, "rb") as file:
            return await file.read()

    async def list_topic(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
    ):
        folder_path = os.path.join(self.base_folder, topic)
        if os.path.exists(folder_path):
            return
        for time_stamp in map(lambda s: datetime.fromtimestamp(float(s)), os.listdir(folder_path)):
            if (start is None or time_stamp >= start) and (end is None or time_stamp <= end):
                yield time_stamp


class ClientSession:
    def __init__(self, server: "Server") -> None:
        self.server: "Server" = server

    async def write_event(
        self,
        topic: str,
        event: bytes,
        time_stamp: Optional[datetime],
        overwrite: bool,
    ):
        time_stamp = time_stamp or datetime.now()
        await self.server.storage.write(topic, str(time_stamp.timestamp()), event, overwrite)
        logging.info(f"Saved event from {self} under: {topic}{time_stamp}")
        self.server.broadcast_to_subscriptions(topic, time_stamp)
        return time_stamp

    async def read_event(self, topic: str, time_stamp: datetime):
        return await self.server.storage.read(topic, str(time_stamp.timestamp()))

    async def read_events(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool,
    ):
        with self.server.subscribe(topic) as subscription:
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


class Server(ServerBase):
    client_session_type = ClientSession

    def __init__(self, event_folder, task_group) -> None:
        super().__init__(ClientSession, task_group)
        self.storage = Storage(event_folder)
