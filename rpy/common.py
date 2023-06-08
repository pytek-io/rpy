import asyncio
import contextlib
import traceback
from typing import Coroutine


@contextlib.contextmanager
def print_error_stack(location):
    try:
        yield
    except Exception:
        traceback.print_exc()
        print(location)
        print("=" * 20)
        raise


@contextlib.contextmanager
def scoped_insert(register, key, value):
    register[key] = value
    try:
        yield key, value
    finally:
        register.pop(key, None)


@contextlib.contextmanager
def scoped_iter(iterable):
    try:
        yield iterable
    finally:
        iterable.close()


closing_scope = scoped_iter


@contextlib.contextmanager
def scoped_execute_coroutine(coroutine: Coroutine):
    task = asyncio.create_task(coroutine)
    try:
        yield task
    finally:
        task.cancel()


class UserException(Exception):
    """Use this to signal expected errors to users."""

    pass
