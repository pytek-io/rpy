import pytest

from tests.utils import (
    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    RemoteObject,
    create_proxy_object_sync,
    test_exception,
)
from tests.utils_sync import enumerate, scoped_iter, sleep




def test_async_generator():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        for i, value in enumerate(proxy.count(10)):
            assert i == value


def test_sync_generator():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        for i, value in enumerate(proxy.count_sync(10)):
            assert i == value


def test_async_generator_exception():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with test_exception() as exception:
            with scoped_iter(proxy.async_generator_exception(exception)) as stream:
                for i, value in enumerate(stream):
                    assert i == value


def test_early_exit():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with scoped_iter(proxy.count(100)) as numbers:
            for i in numbers:
                if i == 3:
                    break
        sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert proxy.finally_called
        # the current value should be 3 since the producer is slower than the consumer
        assert proxy.current_value == 3


def test_overflow():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            with scoped_iter(proxy.count_to_infinity_nowait()) as numbers:
                for i in numbers:
                    sleep(0.1)
        assert ASYNC_GENERATOR_OVERFLOWED_MESSAGE in e_info.value.args[0]


def test_remote_generator_pull_decorator():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        for i, value in enumerate(proxy.remote_generator_pull_synced()):
            sleep(0.1)
            assert i == value
            if i == 3:
                break
