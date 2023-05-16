import argparse
from datetime import datetime, timedelta
import logging
import random

import anyio

from fountainhead import create_async_client, Client


async def write_events(client, topic):
    while True:
        await anyio.sleep(random.random() * 5)
        time_stamp = await client.write_event(topic, {"origin": "sftp", "s3": "fdsljd"})
        logging.info(f"Saved {topic} event at {time_stamp}")


async def subscribe_to_events(client: Client, topic):
    start = datetime.now() - timedelta(minutes=100)
    async with client.read_events(topic, start.timestamp(), None) as events:
        async for time_stamp, content in events:
            print(f"Received {topic} {time_stamp} {content}")


async def main_async(args):
    async with create_async_client(args.host, args.port, "client") as client:
        async with anyio.create_task_group() as task_group:
            for i in range(1):
                topic = f"uploads/client_{i}"
                task_group.start_soon(write_events, client, topic)
                task_group.start_soon(subscribe_to_events, client, topic)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Client test")
    parser.add_argument("host", type=str, help="server host name", nargs="?", default="localhost")
    parser.add_argument("port", type=int, help="tcp port", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(main_async, args)
