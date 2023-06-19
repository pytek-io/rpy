from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, AsyncIterator, Tuple


class Connection(AsyncIterator[Any], metaclass=ABCMeta):
    @abstractmethod
    async def send(self, message: Tuple[Any, ...]):
        ...

    @abstractmethod
    def send_nowait(self, message: Tuple[Any, ...]):
        ...

    @abstractmethod
    async def drain(self):
        ...

    @abstractmethod
    def close(self):
        ...
