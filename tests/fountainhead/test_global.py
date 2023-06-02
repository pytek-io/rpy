import contextlib
import sys
import tempfile
from pickle import dumps
from typing import AsyncIterator, List

import pytest

from fountainhead.client import OVERRIDE_ERROR_MESSAGE, ClientSession
from fountainhead.server import Server


if sys.version_info < (3, 10):
    from asyncstdlib import anext


@contextlib.asynccontextmanager
async def create_test_environment(
    nb_clients: int = 1,
) -> AsyncIterator[List[ClientSession]]:
    with tempfile.TemporaryDirectory() as event_folder:
        server = Server(event_folder)
    yield [ClientSession(server) for _ in range(nb_clients)]


@pytest.mark.anyio
async def test_read_and_write():
    async with create_test_environment() as (client_session,):
        topic, original_value = "topic/subtopic", dumps([123])
        time_stamp = await client_session.write_event(topic, original_value, None, False)
        returned_value = await client_session.read_event(topic, time_stamp)
        assert original_value is not returned_value
        assert original_value == returned_value


@pytest.mark.anyio
async def test_override():
    """Checking override fails if override flag is not set, succeed otherwise."""
    async with create_test_environment() as (client_session,):
        topic, original_value = "topic/subtopic", dumps([123])
        time_stamp = await client_session.write_event(topic, original_value, None, False)
        new_value = dumps("hello world")
        with pytest.raises(Exception) as e_info:
            await client_session.write_event(topic, new_value, time_stamp, False)
        assert e_info.value.args[0] == OVERRIDE_ERROR_MESSAGE
        new_time_stamp = await client_session.write_event(topic, new_value, time_stamp, overwrite=True)
        assert new_time_stamp == time_stamp
        returned_value = await client_session.read_event(topic, time_stamp)
        assert new_value is not returned_value
        assert new_value == returned_value


@pytest.mark.anyio
async def test_subscription():
    async with create_test_environment(2) as (client_session_1, client_session_2):
        topic = "topic/subtopic"
        events = client_session_2.read_events(topic)
        for i in range(10):
            value = dumps({"value": i})
            time_stamp = await client_session_1.write_event(topic, value)
            event_time_stamp, event_value = await anext(events)
            assert event_time_stamp == time_stamp
            assert event_value is not value
            assert value == event_value
