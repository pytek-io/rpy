from typing import Any, AsyncIterator

import anyio
import anyio.abc
import asyncstdlib
import pytest

from dyst import UserException, remote_iter
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ERROR_MESSAGE,
    create_test_environment,
    update_running_tasks,
)


class ClientSession:
    def __init__(self, server):
        self.server = server
        self.ran_tasks = 0

    @remote_iter
    async def count(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i

    @remote_iter
    @update_running_tasks
    async def stream_exception(self, exception) -> AsyncIterator[int]:
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise exception


@pytest.mark.anyio
async def test_stream_count():
    async with create_test_environment(ClientSession) as (proxy, _actual_object):
        async for i, value in asyncstdlib.enumerate(proxy.count(10)):
            assert i == value


@pytest.mark.anyio
async def test_stream_exception():
    async with create_test_environment(ClientSession) as (proxy, actual_object):
        with pytest.raises(Exception) as e_info:
            async with asyncstdlib.scoped_iter(
                proxy.stream_exception(UserException(ERROR_MESSAGE))
            ) as stream:
                async for i, value in asyncstdlib.enumerate(stream):
                    assert i == value
        print("here")
        # assert e_info.value.args[0] == ERROR_MESSAGE
        # assert actual_object.ran_tasks == 1


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
