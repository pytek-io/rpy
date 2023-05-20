import argparse
import logging
import signal

import anyio
import websockets

from fountainhead.server import Server


async def serve(folder, port):
    async with anyio.create_task_group() as task_group:
        server = Server(folder, task_group)
        async with websockets.serve(server.manage_client_session_raw, "localhost", port):
            with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
                async for signum in signals:
                    logging.info(
                        "Ctrl+C pressed."
                        if signum == signal.SIGINT
                        else "Receieved termination signal."
                    )
                    task_group.cancel_scope.cancel()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="Transactor", description="store events")
    parser.add_argument("folder", type=str, help="Stored events location", default="events")
    parser.add_argument("port", type=int, help="tcp port to use", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(serve, args.folder, args.port)
