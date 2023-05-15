import anyio
import pytest
import tempfile
import contextlib
from fountainhead.client_async import Client
from fountainhead.server import Server
from .utils import create_test_connection


@contextlib.asynccontextmanager
async def create_test_environment():
    async with anyio.create_task_group() as task_group:
        with tempfile.TemporaryDirectory() as event_folder:
            server = Server(event_folder, task_group)
            first, second = create_test_connection()
            client = Client(task_group, first, name="dummy")
            task_group.start_soon(client.process_messages_from_server)
            task_group.start_soon(server.manage_client_session, second)
            yield server, client
            task_group.cancel_scope.cancel()


@pytest.mark.anyio
async def test_write_and_read():
    async with create_test_environment() as environ:
        server, client = environ
        topic, original_value = "whatever", [123]
        ts = await client.save_event(topic, original_value)
        returned_value = await client.read_event(topic, ts)
        assert original_value is not returned_value and original_value == returned_value
