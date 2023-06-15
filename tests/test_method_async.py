import pytest
from rmy import UserException
from tests.utils import (
    ERROR_MESSAGE,
    create_proxy_object_async,
    RemoteObject,
)
from tests.utils_async import sleep

pytestmark = pytest.mark.anyio


async def test_coroutine():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo(value)
        assert returned_value is not value
        assert returned_value == value


async def test_coroutine_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(UserException) as e_info:
            await proxy.throw_exception(UserException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE
