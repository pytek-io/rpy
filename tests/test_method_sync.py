import pytest
from dyst import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_proxy_object_sync,
    RemoteObject,
)
from tests.utils_sync import sleep


def test_coroutine():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo(value)
        assert returned_value is not value
        assert returned_value == value


def test_coroutine_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(UserException) as e_info:
            proxy.throw_exception(UserException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE
