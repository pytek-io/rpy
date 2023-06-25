import pytest

from rmy import RemoteException
from tests.utils import ERROR_MESSAGE, RemoteObject, create_proxy_object_sync


def test_async_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine(value)
        assert returned_value is not value
        assert returned_value == value


def test_async_method_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(RemoteException) as e_info:
            proxy.throw_exception_coroutine(RemoteException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE


def test_sync_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_sync(value)
        assert returned_value is not value
        assert returned_value == value


def test_sync_method_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(RemoteException) as e_info:
            proxy.throw_exception_coroutine(RemoteException(ERROR_MESSAGE))
        assert e_info.value.args[0] == ERROR_MESSAGE
