import argparse
import logging
import anyio.abc
import anyio
from rpy import start_tcp_server
from fountainhead.server import Server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="Transactor", description="store events")
    parser.add_argument(
        "folder", type=str, nargs="?", help="Stored events location", default="events"
    )
    parser.add_argument("port", type=int, help="tcp port to use", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(start_tcp_server, args.port, Server(args.folder))
