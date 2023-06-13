from examples.server import Demo
import rmy


if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        proxy: Demo = client.fetch_remote_object()
        print(proxy.greet("world"))
