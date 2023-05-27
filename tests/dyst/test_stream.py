import contextlib
from dataclasses import dataclass
from functools import partial
from typing import Any, AsyncIterator, List

import anyio
import anyio.abc
import asyncstdlib
import pytest

from dyst import ClientSessionBase, ServerBase, UserException
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ERROR_MESSAGE,
    create_test_environment_core,
)


class ClientSession:
    def __init__(self, server, session_core: ClientSessionBase, name: str) -> None:
        self.name: str = name
        self.session_core = session_core
        self.server = server
        self.running_tasks = 0
        self.ran_tasks = 0

    async def count(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i

    async def stream_exception(self, exception):
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise exception


@contextlib.asynccontextmanager
async def create_test_environment():
    async with create_test_environment_core(partial(ServerBase, ClientSession)) as (
        server,
        clients,
    ):
        yield server, clients


@pytest.mark.anyio
async def test_stream_count():
    async with create_test_environment() as (_server, (client,)):
        async with client.subscribe_stream(ClientSession.count, (10,)) as events:
            async for i, value in asyncstdlib.enumerate(events):
                assert i == value


@pytest.mark.anyio
async def test_stream_exception():
    async with create_test_environment() as (_server, (client,)):
        with pytest.raises(Exception) as e_info:
            async with client.subscribe_stream(
                ClientSession.stream_exception, (UserException(ERROR_MESSAGE),)
            ) as events:
                async for i, value in asyncstdlib.enumerate(events):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE


# @pytest.mark.anyio
# async def test_stream_cancellation():
#     async with create_test_environment(partial(ServerBase, ClientSession), AsyncClient) as environ:
#         client: AsyncClientCore = environ.clients[0].client
#         client_session = environ.server.sessions[client.name]
#         async with anyio.create_task_group():
#             with anyio.move_on_after(1):
#                 async with client.create_cancellable_stream(
#                     ClientSession.count, (100,)
#                 ) as numbers:
#                     async for i in numbers:
#                         print(i)
#         await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
#         assert client_session.running_tasks == 0 and client_session.ran_tasks == 1
