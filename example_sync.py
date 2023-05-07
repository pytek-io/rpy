import argparse
import datetime

from fountainhead import create_sync_client


def main(args):
    with create_sync_client(args.host, args.port) as client:
        topic = f"uploads/client_{0}"
        start = datetime.datetime.now() - datetime.timedelta(minutes=1)
        with client.read_events(topic, start.timestamp(), None) as events:
            for time_stamp, value in events:
                print(time_stamp, value)
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Client test")
    parser.add_argument(
        "host", type=str, help="server host name", nargs="?", default="localhost"
    )
    parser.add_argument("port", type=int, help="tcp port", nargs="?", default=8765)
    args = parser.parse_args()
    main(args)
