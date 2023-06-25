import pytest

from tests.utils import ERROR_MESSAGE, RemoteObject, create_proxy_object_async, test_exception


pytestmark = pytest.mark.anyio


async def test_async_method():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo_coroutine(value)
        assert returned_value is not value
        assert returned_value == value


async def test_async_method_exception():
    with test_exception() as exception:
        async with create_proxy_object_async(RemoteObject()) as proxy:
            await proxy.throw_exception_coroutine(exception)



async def test_sync_method():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo_sync(value)
        assert returned_value is not value
        assert returned_value == value


async def test_sync_method_exception():
    with test_exception() as exception:
        async with create_proxy_object_async(RemoteObject()) as proxy:
            await proxy.throw_exception_coroutine(exception)
