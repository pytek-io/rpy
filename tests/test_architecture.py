import anyio
import asyncstdlib
import pytest
from functools import partial
from fountainhead.client import AsyncClientCore
from fountainhead.server import ClientSessionBase, ServerBase, UserException
from .utils import create_test_environment
from typing import AsyncIterator

FINALLY_CALLED = "finally called"
ERROR_MESSAGE = "an error occured"


class ClientSession:
    def __init__(self, server, session_core: ClientSessionBase) -> None:
        self.session_core = session_core
        self.server = server

    async def echo(self, message: str):
        await anyio.sleep(0)
        return message

    async def count(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            await anyio.sleep(0)
            yield i

    async def stream_exception(self, bound: int):
        for i in range(bound):
            await anyio.sleep(0)
            yield i
            if i == 3:
                raise UserException(ERROR_MESSAGE)


class AsyncClient:
    def __init__(self, client: AsyncClientCore) -> None:
        self.client = client

    async def echo(self, message: str):
        return await self.client.send_command(ClientSession.echo, (message,))

    def count(self, bound: int):
        return self.client.subscribe_stream(ClientSession.count, (bound,))

    def stream_exception(self, bound: int):
        return self.client.subscribe_stream(ClientSession.stream_exception, (bound,))


@pytest.mark.anyio
async def test_simple():
    async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
        client: AsyncClient = environ.clients[0]
        value = "test"
        assert await client.echo(value) == value
        async with client.count(5) as events:
            async for i, value in asyncstdlib.enumerate(events):
                print(i, value)
                # assert i == value
        # with pytest.raises(Exception) as e_info:
        #     async with client.stream_exception(10) as events:
        #         async for i, value in asyncstdlib.enumerate(events):
        #             print(i, value)
        # print(e_info.value.args[0])
        # assert e_info.value.args[0] == ERROR_MESSAGE
