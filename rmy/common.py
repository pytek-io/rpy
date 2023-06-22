import asyncio
import contextlib
import signal
import traceback
from typing import Coroutine

import anyio
import anyio.abc


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


@contextlib.contextmanager
def cancel_task_on_exit(coroutine: Coroutine):
    task = asyncio.create_task(coroutine)
    try:
        yield task
    finally:
        task.cancel()


async def cancel_task_group_on_signal(task_group: anyio.abc.TaskGroup):
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                print("Ctrl+C pressed!")
            else:
                print(f"Received signal {signum}, terminating.")

            task_group.cancel_scope.cancel()
            return


class UserException(Exception):
    """Use this to signal expected errors to users."""

    pass
