from typing import Iterator

import anyio
import anyio.abc
import asyncstdlib
import pytest

from dyst import UserException, scoped_iter
from tests.utils import (
    A_LITTLE_BIT_OF_TIME,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    ERROR_MESSAGE,
    create_test_proxy_async_object,
    create_test_proxy_sync_object,
)


class RemoteObject:
    def __init__(self, server):
        self.server = server
        self.current_value = 0
        self.finally_called = False

    def count(self, bound: int) -> Iterator[int]:
        try:
            for i in range(bound):
                anyio.sleep(A_LITTLE_BIT_OF_TIME)
                self.current_value = i
                yield i
        finally:
            self.finally_called = True

    def stream_exception(self, exception) -> Iterator[int]:
        for i in range(10):
            anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                raise exception


@pytest.mark.anyio
def test_async_generator():
    with create_test_proxy_async_object(RemoteObject) as proxy:
        for value in proxy.count(10):
            print(value)
            # assert i == value


@pytest.mark.anyio
def test_stream_exception():
    with create_test_proxy_async_object(RemoteObject) as proxy:
        with pytest.raises(Exception) as e_info:
            with asyncstdlib.scoped_iter(
                proxy.stream_exception(UserException(ERROR_MESSAGE))
            ) as stream:
                for i, value in asyncstdlib.enumerate(stream):
                    assert i == value
        assert e_info.value.args[0] == ERROR_MESSAGE


@pytest.mark.anyio
def test_stream_cancellation():
    with create_test_proxy_async_object(RemoteObject) as proxy:
        with anyio.create_task_group():
            with anyio.move_on_after(1):
                with asyncstdlib.scoped_iter(proxy.count(100)) as numbers:
                    for i in numbers:
                        pass
        anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert proxy.finally_called


@pytest.mark.anyio
def test_stream_early_exit():
    with create_test_proxy_async_object(RemoteObject) as proxy:
        with asyncstdlib.scoped_iter(proxy.count(100)) as numbers:
            for i in numbers:
                if i == 3:
                    break
    anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
    # print(await proxy.finally_called) FIXME: This is not working


def test_stream_early_exit_sync():
    with create_test_proxy_sync_object(RemoteObject) as proxy:
        with scoped_iter(proxy.count(100)) as numbers:
            for i in numbers:
                if i == 3:
                    break
    # await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)


def test_sync():
    with create_test_proxy_sync_object(RemoteObject) as client:
        # assert client.dummy == 0
        # assert client.attribute == 0
        # assert client.add_numbers(1, 2) == 3
        for i, m in enumerate(client.count(5)):
            assert i == m
