import argparse
import logging
import signal
import anyio.abc
import anyio
import asyncio

from fountainhead.server import Server


async def signal_handler(scope: anyio.abc.CancelScope):
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                print("Ctrl+C pressed!")
            else:
                print("Terminated!")

            scope.cancel()
            return


async def serve(folder, port):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(signal_handler, task_group.cancel_scope)
        server = Server(folder, task_group)
        tcp_server = await asyncio.start_server(server.on_new_connection_raw, "localhost", port)
        async with tcp_server:
            await tcp_server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="Transactor", description="store events")
    parser.add_argument("folder", type=str, nargs="?", help="Stored events location", default="events")
    parser.add_argument("port", type=int, help="tcp port to use", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(serve, args.folder, args.port)
