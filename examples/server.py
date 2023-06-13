from rmy import run_tcp_server


class Demo:
    async def greet(self, name):
        return f"Hello {name}"


if __name__ == "__main__":
    run_tcp_server(8080, Demo())
