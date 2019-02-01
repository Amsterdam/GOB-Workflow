from unittest import TestCase, mock

from gobworkflow.storage.auto_reconnect_wrapper import AutoReconnector, auto_reconnect_wrapper

def raise_exception():
    raise Exception("Exception")

def get_n_times_function(n, on_fail, on_success):
    def n_times():
        nonlocal n
        if n > 0:
            n -= 1
            return on_fail()
        else:
            return on_success()
    return n_times

class TestAutoReconnect(TestCase):

    def setUp(self):
        pass

    def test_init(self):
        obj = AutoReconnector(None, None, None)
        self.assertIsNotNone(obj)

    def test_disconnect(self):
        mock_disconnect = mock.MagicMock()
        obj = AutoReconnector(None, None, mock_disconnect)
        obj.disconnect()
        mock_disconnect.assert_called()

    def test_connect(self):
        mock_connect = mock.MagicMock()
        obj = AutoReconnector(is_connected=None, connect=mock_connect, disconnect=None)
        result = obj.connect()
        mock_connect.assert_called()

    def test_exec(self):
        is_connected = lambda: True
        mock_connect = mock.MagicMock()
        mock_disconnect = mock.MagicMock()
        obj = AutoReconnector(is_connected=is_connected, connect=mock_connect, disconnect=mock_disconnect)

        f = lambda: "exec"
        result = obj.exec(f)
        self.assertEqual(result, "exec")
        mock_connect.assert_not_called()
        mock_disconnect.assert_not_called()

    @mock.patch("gobworkflow.storage.auto_reconnect_wrapper.RECONNECT_INTERVAL", 0)
    def test_exec_fails(self):
        is_connected = lambda: False
        mock_connect = get_n_times_function(2, lambda: False, lambda: True)
        mock_disconnect = mock.MagicMock()
        obj = AutoReconnector(is_connected=is_connected, connect=mock_connect, disconnect=mock_disconnect)

        f = get_n_times_function(2, lambda: raise_exception(), lambda: "exec")
        result = obj.exec(f)
        self.assertEqual(result, "exec")
        mock_disconnect.assert_called()

    def test_exec_fails_otherwise(self):
        is_connected = lambda: True
        obj = AutoReconnector(is_connected=is_connected, connect=None, disconnect=None)

        with self.assertRaises(Exception):
            obj.exec(lambda: raise_exception())

class TestAutoReconnectWrapper(TestCase):

    def setUp(self):
        pass

    def test_create(self):
        wrapper = auto_reconnect_wrapper(None, None, None)
        # self.assertIsInstance(wrapper, function)

    def test_exec(self):
        is_connected = lambda: True
        wrapper = auto_reconnect_wrapper(is_connected, None, None)
        f = lambda: "exec"
        result = wrapper(f)()
        self.assertEqual(result, "exec")
