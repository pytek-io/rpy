import pytest

from rmy import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
    RemoteObject,
    create_proxy_object_sync,
)
from tests.utils_sync import enumerate, scoped_iter, sleep, sleep_forever



def test_simple_iteration():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        for i, value in enumerate(proxy.count(10)):
            assert i == value


def test_stream_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            with scoped_iter(
                proxy.generator_exception(UserException(ERROR_MESSAGE))
            ) as stream:
                for i, value in enumerate(stream):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE


def test_stream_early_exit():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with scoped_iter(proxy.count(100)) as numbers:
            for i in numbers:
                if i == 3:
                    break
        sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert proxy.finally_called
        assert proxy.current_value == 3


def test_overflow():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            for i in proxy.count_nowait(120):
                sleep(0.1)
        # assert e_info.value.args[0] == ASYNC_GENERATOR_OVERFLOWED_MESSAGE
