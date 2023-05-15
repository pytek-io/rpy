import argparse
import datetime
import logging
import os
import signal
import traceback
from typing import Dict, Set

import anyio
import websockets
from collections import defaultdict
from .connection import Connection
from .common import print_stack_if_error_and_carry_on

SUBSCRIPTION_BUFFER_SIZE = 100


class Client:
    def __init__(self, server, name, connection: Connection) -> None:
        self.task_group = None
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.server: Server = server

    async def send(self, *args):
        await self.connection.send(args)

    async def evaluate(self, request_id, coroutine):
        send_termination = True
        try:
            success, result = True, await coroutine
        except anyio.get_cancelled_exc_class():
            send_termination = False
            raise
        except:
            success, result = False, traceback.format_exc()
        if send_termination:
            await self.send(request_id, success, result)

    async def evaluate_stream(self, request_id, stream):
        success = True
        try:
            async for result in stream:
                await self.send(request_id, True, result)
            result = None
        except anyio.get_cancelled_exc_class():
            raise
        except:
            success, result = False, traceback.format_exc()
        finally:
            await self.send(request_id, success, result)

    async def cancellable_task_runner(self, request_id, command, details):
        command_or_stram, method = self.server.commands[command]
        async with anyio.create_task_group() as self.running_tasks[request_id]:
            try:
                self.running_tasks[request_id].start_soon(
                    self.evaluate if command_or_stram else self.evaluate_stream,
                    request_id,
                    method(self, request_id, *details),
                )
            finally:
                self.running_tasks.pop(request_id, None)

    async def manage_session(self):
        async for request_id, command, details in self.connection:
            if command is None:
                task_group = self.running_tasks.get(request_id)
                logging.info(f"{self.name} cancelling subscription {request_id}")
                if task_group:
                    task_group.cancel_scope.cancel()
            else:
                self.task_group.start_soon(
                    self.cancellable_task_runner, request_id, command, details
                )

    async def run(self):
        async with anyio.create_task_group() as self.task_group:
            self.task_group.start_soon(self.manage_session)
            await self.connection.wait_closed()
            await self.task_group.cancel_scope.cancel()
            logging.info(f"{self.name} disconnected")


class ServerBase:
    def __init__(self, event_folder, task_group) -> None:
        self.task_group = task_group
        self.event_folder = event_folder
        self.commands = {
            "save_event": (True, self.save_event),
            "read_event": (True, self.read_event),
            "read_events": (False, self.read_events),
        }
        self.subscriptions: Dict[str, Set] = defaultdict(set)

    async def manage_client_session(self, connection: Connection):
        (name,) = await connection.recv()
        logging.info(f"{name} connected")
        await Client(self, name, connection).run()

    async def manage_client_session_raw(self, raw_websocket):
        await self.manage_client_session(Connection(raw_websocket))

    async def save_event(self, client: Client, request_id: int, topic: str, event):
        if topic.startswith("/"):
            topic = topic[1:]
        folder_path = os.path.join(self.event_folder, topic)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        time_stamp = datetime.datetime.now().timestamp()
        file_path = os.path.join(folder_path, str(time_stamp))
        async with await anyio.open_file(file_path, "wb") as file:
            await file.write(event)
        logging.info(f"Saved event from {client} under: {file_path}")
        for request_id, subscription in self.subscriptions[topic]:
            try:
                subscription.send_nowait(time_stamp)
            except anyio.WouldBlock:
                with print_stack_if_error_and_carry_on():
                    client.running_tasks[request_id].cancel_scope.cancel()
        return time_stamp


class Server(ServerBase):
    async def read_events(
        self,
        _client: Client,
        request_id: int,
        topic: str,
        start: datetime.datetime,
        end: datetime.datetime,
        time_stamps_only: bool,
    ):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.subscriptions[topic]
        try:
            subscriptions[topic].add((request_id, sink))
            folder_path = os.path.join(self.event_folder, topic)
            if os.path.exists(folder_path):
                time_stamps = [
                    time_stamp
                    for time_stamp in map(float, os.listdir(folder_path))
                    if time_stamp > start
                    and (True if end is None else time_stamp < end)
                ]
                if time_stamps_only:
                    result = time_stamps
                else:
                    result = []
                    for time_stamp in time_stamps:
                        result.append(
                            (
                                time_stamp,
                                await self.read_event(
                                    self, request_id, topic, time_stamp
                                ),
                            )
                        )
                yield result
            while end is None or datetime.datetime.now().timestamp() < end:
                time_stamp = await stream.receive()
                yield [
                    time_stamp
                    if time_stamps_only
                    else (time_stamp, await self.read_event(topic, str(time_stamp)))
                ]
        finally:
            subscriptions.remove(sink)

    async def read_event(
        self, _client: Client, request_id: int, topic: str, time_stamp: float
    ):
        file_path = os.path.join(self.event_folder, topic, str(time_stamp))
        async with await anyio.open_file(file_path, "rb") as file:
            return await file.read()


async def serve(folder, port):
    async with anyio.create_task_group() as task_group:
        server = Server(folder, task_group)
        async with websockets.serve(server.manage_client_session, "localhost", port):
            with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
                async for signum in signals:
                    logging.info(
                        "Ctrl+C pressed."
                        if signum == signal.SIGINT
                        else "Receieved termination signal."
                    )
                    task_group.cancel_scope.cancel()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="Transactor", description="store events")
    parser.add_argument(
        "folder", type=str, help="Stored events location", default="events"
    )
    parser.add_argument(
        "port", type=int, help="tcp port to use", nargs="?", default=8765
    )
    args = parser.parse_args()
    anyio.run(serve, args.folder, args.port)
