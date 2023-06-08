from rpy import UserException
import anyio
import os
from datetime import datetime
from typing import Any, Optional


OVERWRITE_ERROR_MESSAGE = "Trying to overwrite an existing event without overwrite set to True."


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
            raise UserException(OVERWRITE_ERROR_MESSAGE)
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
        if not os.path.exists(folder_path):
            return
        for time_stamp in map(lambda s: datetime.fromtimestamp(float(s)), os.listdir(folder_path)):
            if (start is None or time_stamp >= start) and (end is None or time_stamp <= end):
                yield time_stamp
