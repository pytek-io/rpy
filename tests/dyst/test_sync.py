import anyio

from dyst import remote, remote_iter
from tests.utils import A_LITTLE_BIT_OF_TIME, create_test_proxy_sync_object


class ClientSession:
    def __init__(self, server):
        self.server = server

    @remote
    async def add_numbers(self, a, b):
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        return a + b

    @remote_iter
    async def async_stream(self, bound):
        for i in range(bound):
            yield i
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)


def test_sync():
    with create_test_proxy_sync_object(ClientSession) as client:
        assert client.add_numbers(1, 2) == 3

        with client.sync_stream(5) as s:
            for i, m in enumerate(s):
                assert i == m

        # with client.sync_stream(5) as s:
        #     for i, m in enumerate(s):
        #         if i == 2:
        #             return
