import pytest
from rmy import UserException
from tests.utils import (
    ERROR_MESSAGE,
    create_proxy_object_sync,
    RemoteObject,
)



def test_coroutine():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine(value)
        assert returned_value is not value
        assert returned_value == value


def test_coroutine_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(UserException) as e_info:
            proxy.throw_exception_coroutine(UserException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE
