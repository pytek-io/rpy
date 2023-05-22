import logging
import os
from datetime import datetime
from typing import Optional

import anyio
import asyncstdlib

from dyst import ClientSessionBase, ServerBase, UserException


OVERRIDE_ERROR_MESSAGE = "Trying to override an existing event without override set to True."


class ClientSession:
    def __init__(self, server: "Server", session_core: ClientSessionBase) -> None:
        self.session_core = session_core
        self.server: "Server" = server

    async def write_event(
        self,
        topic: str,
        event: bytes,
        time_stamp: Optional[datetime],
        override: bool,
    ):
        if topic.startswith("/"):
            topic = topic[1:]
        folder_path = os.path.join(self.server.event_folder, topic)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        time_stamp = time_stamp or datetime.now()
        file_path = os.path.join(folder_path, str(time_stamp.timestamp()))
        if os.path.exists(file_path) and not override:
            raise UserException(OVERRIDE_ERROR_MESSAGE)
        async with await anyio.open_file(file_path, "wb") as file:
            await file.write(event)
        logging.info(f"Saved event from {self} under: {file_path}")
        self.session_core.broadcast_to_subscrptions(topic, time_stamp)
        return time_stamp

    async def read_event(self, topic: str, time_stamp: datetime):
        file_path = os.path.join(self.server.event_folder, topic, str(time_stamp.timestamp()))
        async with await anyio.open_file(file_path, "rb") as file:
            return await file.read()

    async def read_events(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool,
    ):
        with self.session_core.subscribe(topic) as subscription:
            folder_path = os.path.join(self.server.event_folder, topic)
            existing_tags = []
            if os.path.exists(folder_path):
                existing_tags = (
                    time_stamp
                    for time_stamp in map(
                        lambda s: datetime.fromtimestamp(float(s)), os.listdir(folder_path)
                    )
                    if (start is None or time_stamp >= start)
                    and (end is None or time_stamp <= end)
                )
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
        self.event_folder = event_folder
