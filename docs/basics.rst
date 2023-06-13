
Building the simplest RMY aplication
------------------------------------

The simplest possible RMY application looks like this::

    class Demo:
        async def greet(self, name):
            return f"Hello {name}!"


    if __name__ == "__main__":
        rmy.run_tcp_server(8080, Demo())

This will expose an instance of the `Demo` object which can be accessed then be accessed as follows::

if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        proxy: Demo = client.fetch_remote_object()
        print(proxy.greet("world"))

The object returned `fetch_remote_object` call is an ad hoc object which has the same interface as the one that has been exposed. One can also remotely access to the object attributes and even iterate asynchronously through data returned by the shared object.
