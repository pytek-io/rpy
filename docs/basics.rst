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


The `remote_greeter` object returned by `fetch_remote_object` is a proxy object which has the *almost* same interface as the one that is being shared which is therefore type-hinted as `Greeter`. As a sharp reader would have noticed the `greet` method is an asynchronous method which we call synchronously. This is because of the fact we connected through a synchronous client which will convert all exposed methods to synchronous. Conversely an asynchronous client, returned by `create_async_client` will convert all methods into asynchronous. Those implicit conversions from synchronous to asynchronous methods and vice-versa will rightfully cause linters to choke. One either can use `as_sync`, `as_async` as decorators or simple adapter methods to keep them happy.

Exception handling
------------------

Remote procedure calls either be used to query or compute something or on to trigger some actions remotely. In both cases the caller will want to know if the remote call did complete successfully, that is whether it raised an exception or not. This is why RMY will always re-raise any exception locally. For example if we modify the `greet` method as follows.

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


Exposing generators
-------------------

One can remotely iterate remotely through data returned by an exposed object. For example we can make our greeting service a bit more friendly by adding the following method to our `Greeter` class.

.. code-block:: python

    import asyncio

    class Greeter:
        ...
        async def chat(self, name):
        for message in [f"Hello {name}!", f"How are you {name}?", f"Goodbye {name}!"]
            yield message
            await asyncio.sleep(1)

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


Iteration policies
------------------

By nature asynchronous systems are usually prone to slow consumer issues which can cause uncontrolled memory use. RMY provides mechanisms to prevent this from happening. It will eagerly iterate through asynchronous generators and send data to the client straightaway. Those data will be buffered by the client. If too many values accumulate, the client code will receive a `BufferFullError` exception. This behaviour can be customized by the `max_data_in_flight_count`  and `max_data_in_flight_size` parameters.

.. code-block:: python

    class Greeter:
        ...
        async def count(self, bound):
            for i in range(bound):
                yield i

If we try to iterate through the results as follows, an `BufferFullError` exception will be thrown after 10 loop iterations on the server. This value is the default value for the maximum number of items that can be buffered by the client. 

.. code-block:: python
    
    import time

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
            for i in proxy.count(1000000):
                time.sleep(1)
                print(i)

One would easily realize that in this example the data should be "pulled" by the client as it consumes it, rather than been "pushed" blindly by the server. This can be done by either by wrapping the generator in a `RemoteGeneratorPull` object or by decorating the method with `remote_generator_pull` as follows.

.. code-block:: python

    class Greeter:
        ...
        @rmy.remote_generator_pull
        async def count(self, bound):
            for i in range(bound):
                yield i


Cancellation and early exits
------------------------------------

Coroutines can be cancelled from the client code. In the following example, the `sleep_forever` method will be cancelled after 1 second. 

.. code-block:: python
    
    import asyncio

    class Greeter:
        async def sleep_forever(self, duration):
            while True:
                await asyncio.sleep(duration)

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy: Greeter = client.fetch_remote_object()
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever(100)



Likewise iterators can be exited early by calling the `close` method on them. This is best done using context manager as follows.

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