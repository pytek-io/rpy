import logging
import os
from datetime import datetime
from typing import ByteString, Optional

import anyio

from dyst import ServerBase, ClientSessionBase, UserException

SUBSCRIPTION_BUFFER_SIZE = 100
OVERRIDE_ERROR_MESSAGE = "Trying to override an existing event without override set to True."


class ClientSession(ClientSessionBase):
    async def write_event(
        self,
        request_id: int,
        topic: str,
        event: ByteString,
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
        for request_id, subscription in self.server.subscriptions[topic]:
            try:
                subscription.send_nowait(time_stamp)
            except anyio.WouldBlock:
                self.running_tasks[request_id].cancel_scope.cancel()
        return time_stamp

    async def read_event(self, _request_id: int, topic: str, time_stamp: datetime):
        file_path = os.path.join(self.server.event_folder, topic, str(time_stamp.timestamp()))
        async with await anyio.open_file(file_path, "rb") as file:
            return await file.read()

    async def read_events(
        self,
        request_id: int,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool,
    ):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.server.subscriptions[topic]
        subscription = (request_id, sink)
        try:
            subscriptions.add(subscription)
            folder_path = os.path.join(self.server.event_folder, topic)
            if os.path.exists(folder_path):
                for time_stamp in map(
                    lambda s: datetime.fromtimestamp(float(s)), os.listdir(folder_path)
                ):
                    if start is not None and time_stamp < start:
                        continue
                    if end is not None and time_stamp > end:
                        continue
                    yield (
                        time_stamp
                        if time_stamps_only
                        else (
                            time_stamp,
                            await self.read_event(request_id, topic, time_stamp),
                        )
                    )
            while end is None or datetime.now() < end:
                time_stamp = await stream.receive()
                yield (
                    time_stamp
                    if time_stamps_only
                    else (
                        time_stamp,
                        await self.read_event(request_id, topic, time_stamp),
                    )
                )
        finally:
            if subscription in subscriptions:
                subscriptions.remove(subscription)


class Server(ServerBase):
    client_session_type = ClientSession

    def __init__(self, event_folder, task_group) -> None:
        super().__init__(task_group)
        self.event_folder = event_folder
