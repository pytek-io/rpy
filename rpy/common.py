import asyncio
import anyio
import contextlib
import traceback
from typing import Coroutine


@contextlib.contextmanager
def print_error_stack():
    try:
        yield
    except anyio.get_cancelled_exc_class():
        raise
    except Exception:
        traceback.print_exc()
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

@contextlib.asynccontextmanager
async def scoped_execute_coroutine_new(method, *args, **kwargs):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(method, *args, **kwargs)
        yield task_group.cancel_scope.cancel


class UserException(Exception):
    """Use this to signal expected errors to users."""

    pass
