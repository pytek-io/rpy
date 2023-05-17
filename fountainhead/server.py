import argparse
import inspect
import logging
import os
import signal
import traceback
from collections import defaultdict
from datetime import datetime
from typing import ByteString, Dict, Optional, Set

import anyio
import websockets

from .common import print_stack_if_error_and_carry_on
from .connection import Connection

SUBSCRIPTION_BUFFER_SIZE = 100


class UserException(Exception):
    pass


class ClientSessionBase:
    """Implements non functional specific details"""

    def __init__(self, server, name, connection: Connection) -> None:
        self.task_group = None
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.server: Server = server

    def __str__(self) -> str:
        return self.name

    async def send(self, *args):
        try:
            await self.connection.send(args)
        except anyio.get_cancelled_exc_class():
            raise
        except:
            traceback.print_exc()

    async def evaluate(self, request_id, coroutine):
        send_termination = True
        try:
            success, result = True, await coroutine
        except anyio.get_cancelled_exc_class():
            send_termination = False
            raise
        except UserException as e:
            success, result = False, e.args[0]
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
        async with anyio.create_task_group() as self.running_tasks[request_id]:
            try:
                test = getattr(self, command)(request_id, *details)
                self.running_tasks[request_id].start_soon(
                    self.evaluate
                    if inspect.isawaitable(test)
                    else self.evaluate_stream,
                    request_id,
                    test,
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
            logging.info(f"{self} disconnected")


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
            raise UserException(
                "Trying to override an existing event without override set to True."
            )
        async with await anyio.open_file(file_path, "wb") as file:
            await file.write(event)
        logging.info(f"Saved event from {self} under: {file_path}")
        for request_id, subscription in self.server.subscriptions[topic]:
            try:
                subscription.send_nowait(time_stamp)
            except anyio.WouldBlock:
                with print_stack_if_error_and_carry_on():
                    self.running_tasks[request_id].cancel_scope.cancel()
        return time_stamp

    async def read_event(self, _request_id: int, topic: str, time_stamp: datetime):
        file_path = os.path.join(
            self.server.event_folder, topic, str(time_stamp.timestamp())
        )
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
        try:
            subscriptions.add((request_id, sink))
            folder_path = os.path.join(self.server.event_folder, topic)
            if os.path.exists(folder_path):
                time_stamps = map(float, os.listdir(folder_path))
                if start is not None:
                    time_stamps = (
                        time_stamp for time_stamp in time_stamps if time_stamp >= start
                    )
                if end is not None:
                    time_stamps = (
                        time_stamp for time_stamp in time_stamps if time_stamp <= end
                    )
                if time_stamps_only:
                    result = time_stamps
                else:
                    result = []
                    for time_stamp in time_stamps:
                        result.append(
                            (
                                time_stamp,
                                await self.read_event(request_id, topic, time_stamp),
                            )
                        )
                yield result
            while end is None or datetime.now() < end:
                time_stamp = await stream.receive()
                yield [
                    time_stamp
                    if time_stamps_only
                    else (
                        time_stamp,
                        await self.read_event(request_id, topic, time_stamp),
                    )
                ]
        finally:
            if sink not in subscriptions:
                subscriptions.remove(sink)


class Server:
    def __init__(self, event_folder, task_group) -> None:
        self.task_group = task_group
        self.event_folder = event_folder
        self.subscriptions: Dict[str, Set] = defaultdict(set)

    async def manage_client_session(self, connection: Connection):
        (name,) = await connection.recv()
        logging.info(f"{name} connected")
        await ClientSession(self, name, connection).run()

    async def manage_client_session_raw(self, raw_websocket):
        await self.manage_client_session(Connection(raw_websocket))


async def serve(folder, port):
    async with anyio.create_task_group() as task_group:
        server = Server(folder, task_group)
        async with websockets.serve(
            server.manage_client_session_raw, "localhost", port
        ):
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
