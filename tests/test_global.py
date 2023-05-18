import contextlib
import tempfile
from typing import AsyncIterable, List

import anyio
import anyio.abc
import pytest
from dataclasses import dataclass
from fountainhead.client_async import AsyncClient
from fountainhead.server import Server

from .utils import create_test_connection


@dataclass
class Environment:
    task_group: anyio.abc.TaskGroup
    server: Server
    clients: List[AsyncClient]


@contextlib.asynccontextmanager
async def create_test_environment(
    nb_clients: int = 1,
) -> AsyncIterable[Environment]:
    clients = []
    async with anyio.create_task_group() as task_group:
        with tempfile.TemporaryDirectory() as event_folder:
            server = Server(event_folder, task_group)
            for i in range(nb_clients):
                first, second = create_test_connection()
                client = AsyncClient(task_group, first, name=f"client_{i}")
                await task_group.start(client.process_messages_from_server)
                task_group.start_soon(server.manage_client_session, second)
                clients.append(client)
            yield Environment(task_group, server, clients)
            task_group.cancel_scope.cancel()


@pytest.mark.anyio
async def test_simple_write_and_read():
    async with create_test_environment() as environ:
        [client] = environ.clients
        topic, original_value = "topic/subtopic", [123]
        time_stamp = await client.write_event(topic, original_value)
        returned_value = await client.read_event(topic, time_stamp)
        assert original_value is not returned_value
        assert original_value == returned_value
        with pytest.raises(Exception) as e_info:
            await client.write_event(topic, original_value, time_stamp)
        print(e_info)


@pytest.mark.anyio
async def test_subscription():
    async with create_test_environment(2) as environ:
        client1, client2 = environ.clients
        topic = "topic/subtopic"
        async with client2.read_events(topic) as events:
            for i in range(1):
                value = {"value": i}
                time_stamp = await client1.write_event(topic, value)
                returned_time_stamp, returned_value = await events.__anext__()
                assert returned_value is not value
                assert returned_time_stamp == time_stamp and value == returned_value
        await anyio.sleep(1)
        print(environ.server.subscriptions[topic])
        await anyio.sleep(1)
