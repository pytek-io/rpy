import pytest
from tests.utils_sync import scoped_iter, enumerate, sleep

from rmy import UserException
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_proxy_object_sync,
    RemoteObject,
)


def test_async_generator():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        for i, value in enumerate(proxy.count(10)):
            assert i == value
        sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert proxy.finally_called


def test_stream_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            with scoped_iter(proxy.generator_exception(UserException(ERROR_MESSAGE))) as stream:
                for i, value in enumerate(stream):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE


def test_stream_early_exit():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with scoped_iter(proxy.count(100)) as numbers:
            for i in numbers:
                if i == 3:
                    break
        sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
