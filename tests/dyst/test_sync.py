import anyio

from dyst import remote, remote_iter
from tests.utils import A_LITTLE_BIT_OF_TIME, create_test_proxy_sync_object


class RemoteObject:
    attribute: int

    def __init__(self, server):
        self.server = server
        self.attribute = 0

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
    with create_test_proxy_sync_object(RemoteObject) as client:
        # assert client.dummy == 0
        # assert client.attribute == 0
        assert client.add_numbers(1, 2) == 3
        for i, m in enumerate(client.async_stream(5)):
            assert i == m
