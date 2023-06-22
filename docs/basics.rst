
Overview
========

Calling a method remotely
-------------------------

The simplest possible RMY application looks like this:

..  code-block:: python

    import rmy

    class Demo:
        async def greet(self, name):
            return f"Hello {name}!"


    if __name__ == "__main__":
        rmy.run_tcp_server(8080, Demo())


The `run_tcp_server` method will expose an instance of the `Demo` object which can then be remotely accessed to as follows.

.. code-block:: python

    import rmy
    from hello_rmy_server import Demo

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            while True:
            print('Enter your name:')
            name = input()
            print(proxy.greet(name))


The object returned `fetch_remote_object` call is a proxy object which has the *almost* same interface as the one that is being shared which is therefore type-hinted as `Demo`. As a sharp reader/linter would notice the `greet` method is an asynchronous method which we call synchronously. This is because of the fact we connected through a synchronous client. If we use `create_async_client` instead, the `greet` method will remain asynchronous. 


Iterating remotely
------------------

One can also remotely iterate asynchronously through data returned by the shared object. For example if we add the following method to our Demo class.

.. code-block:: python

    import asyncio

    class Demo:
        ...
        async def chat(self, name):
            yield f"Hello {name}!"
            await asyncio.sleep(1)
            yield f"How are you {name}?"
            await asyncio.sleep(1)
            yield f"Goodbye {name}!"

Then we can iterate through the results as follows, and see each server answers being printed one second apart.
    
.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            while True:
                print('Enter your name:')
                name = input()
                for sentence in proxy.chat(name):
                    print(sentence)


Accessing object attributes remotely
-------------------------------------

One can also read and write remote object attributes as follows.

.. code-block:: python

    class Demo:
        def __init__(self):
            self.greet = "Hello"

        async def greet(self, name):
            return f"{self.greet} {name}!"

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            print("Current greeting", proxy.greet)
            proxy.name = "Hi"
            print(proxy.greet("John"))

Exception handling
------------------

Remote procedure calls provide among other things a convenient way to trigger actions on another machine. For this work reliably the result is always returned to the caller. If the remote procedure call raises an exception, the exception is propagated to the caller. For example if we modify the `greet` method as follows.

.. code-block:: python

    class Demo:

        async def greet(self, name):
            if not name:
                raise ValueError("Name cannot be empty")
            return f"{self.greet} {name}!"

Then the following code will print the exception message.

.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            try:
                print(proxy.greet(""))
            except Exception as e:
                print(e)


Loop synchronization
--------------------

Because of their very nature asynchronous iterators are prone to synchronization issues in which the producer is faster than the consumer. This cause data to accumulate in some part of the system and can lead to out of memory errors if not properly controlled. To ensure maximum stability, RMY will always eagerly push data to the client which buffers them. If a buffer becomes full, the client code will receive a `BufferFullError` exception.

.. code-block:: python

    class Demo:
        ...
        async def count(self):
            for i in range(1000000):
                yield i

Then if we try to iterate through the results as follows, we will get a `BufferFullError` exception.

.. code-block:: python
    
    import time

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            for i in proxy.count():
                time.sleep(1)
                print(i)

To avoid this issue, we can either increase the buffer size. Note that in this slightly contrieved example, the exposed generator is asynchronous although it does not really need to be so. In this case we can simply use a synchronous generator which will synchronized with the client code avoiding any of this issues.


Cancellation and early exits
------------------------------------

Coroutines can be cancelled from the client code. In the following example, the `sleep_forever` method will be cancelled after 1 second. 

.. code-block:: python
    
        import asyncio
    
        class Demo:
            async def sleep_forever(self):
                while True:
                    await asyncio.sleep(1)
    
        if __name__ == "__main__":
            with rmy.create_sync_client("localhost", 8080) as client:
                proxy: Demo = client.fetch_remote_object()
            async with anyio.create_task_group():
                with anyio.move_on_after(1):
                    await proxy.sleep_forever()



In the same vein iterators can be exited early by calling the `close` method on them. This is best done using context manager as follows.

.. code-block:: python

    import asyncio

    class Demo:
        async def count(self):
            for i in range(1000000):
                yield i

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Demo = client.fetch_remote_object()
            async with proxy.count() as it:
                async for i in it:
                    print(i)
                    if i == 10:
                        it.close()