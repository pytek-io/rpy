import tempfile
from functools import partial
import anyio
import anyio.abc
import sys

import pytest

from fountainhead.client import AsyncClient
from fountainhead.server import OVERRIDE_ERROR_MESSAGE, Server

from .utils import create_test_environment

if sys.version_info < (3, 10):
    from asyncstdlib import anext


@pytest.mark.anyio
async def test_read_and_write():
    with tempfile.TemporaryDirectory() as event_folder:
        async with create_test_environment(partial(Server, event_folder), AsyncClient) as environ:
            [client] = environ.clients
            topic, original_value = "topic/subtopic", [123]
            time_stamp = await client.write_event(topic, original_value)
            returned_value = await client.read_event(topic, time_stamp)
            assert original_value is not returned_value
            assert original_value == returned_value


@pytest.mark.anyio
async def test_override():
    """Checking override fails if override flag is not set, succeed otherwise."""
    with tempfile.TemporaryDirectory() as event_folder:
        async with create_test_environment(partial(Server, event_folder), AsyncClient) as environ:
            [client] = environ.clients
            topic, original_value = "topic/subtopic", [123]
            time_stamp = await client.write_event(topic, original_value)
            new_value = "hello world"
            with pytest.raises(Exception) as e_info:
                await client.write_event(topic, new_value, time_stamp)
            assert e_info.value.args[0] == OVERRIDE_ERROR_MESSAGE
            new_time_stamp = await client.write_event(topic, new_value, time_stamp, override=True)
            assert new_time_stamp == time_stamp
            returned_value = await client.read_event(topic, time_stamp)
            assert new_value is not returned_value
            assert new_value == returned_value


@pytest.mark.anyio
async def test_subscription():
    with tempfile.TemporaryDirectory() as event_folder:
        async with create_test_environment(
            partial(Server, event_folder), AsyncClient, 2
        ) as environ:
            client1, client2 = environ.clients
            topic = "topic/subtopic"
            async with client2.read_events(topic) as events:
                for i in range(10):
                    value = {"value": i}
                    time_stamp = await client1.write_event(topic, value)
                    event_time_stamp, event_value = await anext(events)
                    assert event_time_stamp == time_stamp
                    assert event_value is not value
                    assert value == event_value
            await anyio.sleep(0)  # letting the server do the house keeping
            assert not environ.server.subscriptions[topic]
