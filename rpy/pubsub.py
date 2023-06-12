from collections import defaultdict
from typing import Any, Dict

import mpmc


class PubSubManager:
    def __init__(self):
        self.subscriptions: Dict[str, mpmc.Broadcast] = defaultdict(mpmc.Broadcast)

    def broadcast_to_subscriptions(self, topic: str, message: Any):
        self.subscriptions[topic].put_nowait(message)

    def subscribe(self, topic: str) -> mpmc.Broadcast:
        return self.subscriptions[topic]
