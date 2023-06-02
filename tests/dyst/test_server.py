import contextlib
from typing import AsyncIterator, List, Tuple

import anyio
import pytest

from dyst import SessionManager, UserException


# @contextlib.asynccontextmanager
# async def create_test_environment_new() -> AsyncIterator[Tuple[ServerBase, List[ClientSession]]]:
#     async with create_test_environment_core(partial(ServerBase, ClientSession)) as (
#         server,
#         clients,
#     ):
#         yield server, [ClientSession(server, name="client_0") for client in clients]

# @pytest.mark.anyio
# async def test_simple():
