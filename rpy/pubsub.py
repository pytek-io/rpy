import contextlib
import logging
from collections import defaultdict
from typing import Any, Dict, Set

import anyio
import anyio.abc


SUBSCRIPTION_BUFFER_SIZE = 100


class PubSubManager:
    def __init__(self):
        self.subscriptions: Dict[str, Set] = defaultdict(set)

    @contextlib.contextmanager
    def subscribe(self, topic: Any):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.subscriptions[topic]
        try:
            subscriptions.add(sink)
            yield stream
        finally:
            if sink in subscriptions:
                subscriptions.remove(sink)

    def broadcast_to_subscriptions(self, topic: Any, message: Any):
        for subscription in self.subscriptions[topic]:
            try:
                subscription.send_nowait(message)
            except anyio.WouldBlock:
                logging.warning("ignoring subscriber which is too far behind")
