import pytest
from tests.utils import (
    create_proxy_object_async,
    RemoteObject,
)


@pytest.mark.anyio
async def test_attribute():
    value = "test"
    async with create_proxy_object_async(RemoteObject(value)) as proxy:
        returned_value = await proxy.attribute
        assert returned_value is not value
        assert returned_value == value


@pytest.mark.anyio
async def test_non_existent_attribute():
    async with create_proxy_object_async(RemoteObject("test")) as proxy:
        with pytest.raises(AttributeError):
            await proxy.dummy
