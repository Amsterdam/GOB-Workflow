import threading
import json
import pika
import pytest

from mock import patch

from gobworkflow.message_broker.async_message_broker import AsyncConnection


class MockIoloop:

    def __init__(self):
        self.lock = threading.Lock()
        self.running = False

    def start(self):
        if not self.running:
            self.lock.acquire()
            self.running = True
            # Wait for stop() to have been called
            self.lock.acquire()
            self.running = False

    def stop(self):
        self.lock.release()


class MockDeliver:

    delivery_tag = "tag"

    def __init__(self, key):
        self.routing_key = key

class MockChannel:

    connection_success = True

    consumer_callback = None
    consumer_tags = [1, 2, 3]
    on_close = None
    is_open = True

    def add_on_close_callback(self, callback):
        self.on_close = callback

    def close(self):
        self.is_open = False
        if self.on_close is not None:
            self.on_close(self, code=0, text="OnClose")

    def basic_publish(self,
                      exchange,
                      routing_key,
                      properties,
                      body):
        global published_message
        published_message = body

    def basic_cancel(self,
                     callback,
                     consumer_tag):
        pass

    def basic_ack(self,
                  tag):
        pass

    def queue_bind(self,
                   callback,
                   exchange,
                   queue,
                   routing_key):
        self.consumer_callback = callback
        callback({})

    def basic_consume(self,
                      consumer_callback,
                      queue):
        self.consumer_callback = consumer_callback
        global consumed_message
        consumed_message = "mybody"
        consumer_callback(self, MockDeliver("mykey"), {}, "mybody")


class MockConnection:

    connection_success = True

    on_close = None
    is_open = True

    def __init__(self):
        self.ioloop = MockIoloop()

    def channel(self, on_open_callback):
        channel = MockChannel()
        channel.connection_succes = self.connection_success
        if self.connection_succes:
            on_open_callback(channel)
        return channel

    def close(self):
        if self.ioloop.running:
            self.ioloop.stop()
        if self.on_close is not None:
            self.on_close(self, code=0, text="OnClose")


class MockPika:

    selectConnectionOK = True

    def SelectConnection(self,
                         parameters,
                         on_open_callback,
                         on_open_error_callback,
                         on_close_callback):
        if self.selectConnectionOK:
            connection = MockConnection()
            connection.connection_succes = True
            connection.on_close = on_close_callback
            on_open_callback(connection)
            return connection
        else:
            on_open_error_callback(None, 'Fail to open connection')
            return None


consumed_message = None
published_message = None
on_connect_called = False
connection_params = {'host': "host", 'credentials': {'username': "username", 'password': "password"}}

def mock_connection(monkeypatch, connection_success):
    _pika = MockPika()
    _pika.selectConnectionOK = connection_success
    monkeypatch.setattr(pika, 'SelectConnection', _pika.SelectConnection)

    global consumed_message
    global published_message
    global on_connect_called

    consumed_message = None
    published_message = None
    on_connect_called = False


def on_connect():
    # Helper function to test callback on connect
    global on_connect_called

    on_connect_called = True


def test_connection_constructor():
    # Test if a connection can be initialized
    connection = AsyncConnection(connection_params)
    assert(connection is not None)
    assert(connection._connection_params == connection_params)


def test_disconnect():
    # Test if a disconnect can be called without an earlier connect
    connection = AsyncConnection('')
    assert(connection.disconnect() is None)


def test_idempotent_disconnect():
    # Test disconnect repeated execution
    connection = AsyncConnection('')
    assert(connection.disconnect() is None)
    assert(connection.disconnect() is None)


def test_connect_failure(monkeypatch):
    # Test if connect reports failure to connect
    mock_connection(monkeypatch, connection_success=False)

    connection = AsyncConnection(connection_params)
    assert(connection.connect() == False)


def test_connect_context_manager(monkeypatch):
    # Test if context manager calls disconnect on exit with statement
    mock_connection(monkeypatch, connection_success=True)

    org_connection=None
    with patch.object(AsyncConnection, 'disconnect') as mocked_disconnect:
        with AsyncConnection(connection_params) as connection:
            org_connection = connection # save to call the real disconnect afterwards
            pass
        assert(mocked_disconnect.called)
    org_connection.disconnect()    # Call the real disconnect


def test_connect_success(monkeypatch):
    # Test if connect reports connection success
    mock_connection(monkeypatch, connection_success=True)

    connection = AsyncConnection(connection_params)
    assert(connection.connect() == True)
    connection.disconnect()


def test_connect_callback_success(monkeypatch):
    # Test if connect calls callback on success
    mock_connection(monkeypatch, connection_success=True)
    global on_connect_called

    assert(not on_connect_called)
    connection = AsyncConnection(connection_params)
    connection.connect(on_connect)
    assert(on_connect_called)
    connection.disconnect()


def test_connect_callback_failure(monkeypatch):
    # Test if connect does not call callback on failure
    mock_connection(monkeypatch, connection_success=False)
    global on_connect_called

    connection = AsyncConnection(connection_params)
    assert(not on_connect_called)
    connection.connect(on_connect)
    assert(not on_connect_called)


def test_publish(monkeypatch):
    # Test publish message
    mock_connection(monkeypatch, connection_success=True)

    connection = AsyncConnection(connection_params)
    connection.connect()
    queue = {
        "exchange": "exchange",
        "name": "name",
        "key": "key"
    }
    connection.publish(queue, "key", "message")
    assert(published_message == json.dumps("message"))
    connection.disconnect()


def test_publish_failure(monkeypatch):
    # Test publish failure
    mock_connection(monkeypatch, connection_success=True)

    connection = AsyncConnection(connection_params)
    queue = {
        "name": "name",
        "key": "key"
    }
    with pytest.raises(Exception):
        connection.publish(queue, "key", "message")


def test_subscribe(monkeypatch):
    # Test subscription and message receipt
    mock_connection(monkeypatch, connection_success=True)

    result = None

    def on_message(self, queue, key, body):
        assert(key == "mykey")
        assert(body == "mybody")

    connection = AsyncConnection(connection_params)
    connection.connect()
    queue = {
        "exchange": "exchange",
        "name": "name",
        "key": "key"
    }
    connection.subscribe([queue, queue], on_message)
    connection.publish(queue, "key", "mybody")
    connection.disconnect()
    assert(consumed_message == "mybody")
