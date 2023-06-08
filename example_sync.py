import argparse
from pickle import dumps
from fountainhead import create_sync_client
import datetime


def main(args):
    with create_sync_client(args.host, args.port) as client:
        topic = f"uploads/client_{0}"
        t = client.write_event(topic, dumps({"whatever": 1}))
        start = datetime.datetime.now() - datetime.timedelta(minutes=1)
        for time_stamp, value in client.read_events(topic, start, None):
            print(time_stamp, value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Client test")
    parser.add_argument("host", type=str, help="server host name", nargs="?", default="localhost")
    parser.add_argument("port", type=int, help="tcp port", nargs="?", default=8765)
    args = parser.parse_args()
    main(args)
