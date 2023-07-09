
Tutorial
========

A simple greeting application
-----------------------------

One can build a friendly greeting service as follows. We pass an object to the server that will be expose it to all clients.

..  code-block:: python

    import rmy

    class Greeter:
        async def greet(self, name):
            return f"Hello {name}!"


    if __name__ == "__main__":
        rmy.run_tcp_server(8080, Greeter())

Client can then access this object as follows.

.. code-block:: python

    import rmy
    from hello_rmy_server import Greeter

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            remote_greeter: Greeter = client.fetch_remote_object()
            while True:
                print('Enter your name:')
                name = input()
                print(remote_greeter.greet(name))


The `remote_greeter` object returned by `fetch_remote_object` is a proxy object which has the *almost* same interface as the one that is being shared which is therefore type-hinted as `Greeter`. As a sharp reader would have noticed the `greet` method is an asynchronous method which we call synchronously. This is because of the fact we connected through a synchronous client which will convert all exposed methods to synchronous. Conversely an asynchronous client, returned by `create_async_client` will convert all methods into asynchronous. Those implicit conversions from synchronous to asynchronous methods and vice-versa will rightfully cause linters to choke. One either use decorators or simple adapter methods to keep them happy. 

Exception handling
------------------

Remote procedure calls either be used in a functional style to query something or on the contrary to causes some side effects remotely. In both cases the caller will want to know if the remote call did complete successfully, that is whether it raised an exception or not. This is why RMY will always re-raise any exception locally. For example if we modify the `greet` method as follows.

.. code-block:: python

    class Greeter:

        async def greet(self, name):
            if not name:
                raise ValueError("Name cannot be empty")
            return f"{self.greet} {name}!"

Then the following code will print the exception message.

.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
            try:
                print(proxy.greet(""))
            except Exception as e:
                print(e)


Exposing generators
-------------------

One can remotely iterate remotely through data returned by the shared object. For example we can make our greeting service a bit more friendly by adding the following method to our `Greeter` class.

.. code-block:: python

    import asyncio

    class Greeter:
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
            proxy: Greeter = client.fetch_remote_object()
            while True:
                print('Enter your name:')
                name = input()
                for sentence in proxy.chat(name):
                    print(sentence)


Accessing object attributes remotely
-------------------------------------

One can also read and write remote object attributes as follows. In our example we can change the greeting message as follows.

.. code-block:: python

    class Greeter:
        def __init__(self):
            self.greet = "Hello"

        async def greet(self, name):
            return f"{self.greet} {name}!"

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
            print("Current greeting", proxy.greet)
            proxy.name = "Hi"
            print(proxy.greet("John"))


Iteration policies
------------------

By nature asynchronous systems are prone to slow consumer issues which can cause run out of memory crashes. RMY provides mechanisms to prevent those. By default RMY will eagerly iterate through asynchronous generators and send data to the client which buffers them. If a buffer becomes full, the client code will receive a `BufferFullError` exception. For example the following code will only send data to the client when the buffer is full.

Because of their very nature asynchronous iterators are prone to synchronization issues in which the producer is faster than the consumer. This cause data to accumulate in some part of the system and can lead to out of memory errors if not properly controlled. RMY will always eagerly iterate through asynchronous generators and send data to the client which buffers them. If a buffer becomes full, the client code will receive a `BufferFullError` exception.

.. code-block:: python

    class Greeter:
        ...
        async def count(self):
            for i in range(1000000):
                yield i

Then if we try to iterate through the results as follows, we will get a `BufferFullError` exception.

.. code-block:: python
    
    import time

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
            for i in proxy.count():
                time.sleep(1)
                print(i)

To avoid this issue, we can either increase the buffer size. Note that in this slightly contrieved example, the exposed generator is asynchronous although it does not really need to be so. In this case we can wrap our async generator in a `RemoteGeneratorPull`.

.. code-block:: python

    class Greeter:
        ...
        async def count(self):
            for i in range(1000000):
                yield i


Cancellation and early exits
------------------------------------

Coroutines can be cancelled from the client code. In the following example, the `sleep_forever` method will be cancelled after 1 second. 

.. code-block:: python
    
        import asyncio
    
        class Greeter:
            async def sleep_forever(self):
                while True:
                    await asyncio.sleep(1)
    
        if __name__ == "__main__":
            with rmy.create_sync_client("localhost", 8080) as client:
                proxy: Greeter = client.fetch_remote_object()
            async with anyio.create_task_group():
                with anyio.move_on_after(1):
                    await proxy.sleep_forever()



In the same vein iterators can be exited early by calling the `close` method on them. This is best done using context manager as follows.

.. code-block:: python

    import asyncio

    class Greeter:
        async def count(self):
            for i in range(1000000):
                yield i

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
            async with proxy.count() as it:
                async for i in it:
                    print(i)
                    if i == 10:
                        it.close()