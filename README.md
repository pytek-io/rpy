A powerful PRC framework to build distributed application easily in Python. 

## How to use it

One can expose a Python object through a server as follow.

``` python
with create_sync_client(server_host, server_port, name="producer") as client:
        topic = "uploads/client_1"
        payload = {"value": 1}
        time_stamp = client.write_event(topic, payload)
```

This will save the payload under *uploads/client_1* topic. Events can be read starting from any point in time.

``` python
with create_sync_client(server_host, server_port, name="consumer") as client:
    with client.read_events("uploads/client_1") as events:
        for time_stamp, value in events:
            print(time_stamp, value)
```

This will display all the events saved under *uploads/client_1* topic from the beginning then will return newly saved ones. You can specify the beginning and the end of the iteration using *start* and *end* arguments.

## 