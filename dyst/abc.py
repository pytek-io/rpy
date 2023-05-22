from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, AsyncIterator, Tuple


class Connection(AsyncIterator[Any], metaclass=ABCMeta):
    @abstractmethod
    async def send(self, message: Tuple[Any, ...]):
        """
        Send a message.
        """
