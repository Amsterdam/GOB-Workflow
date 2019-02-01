"""Auto reconnect wrapper

A wrapper that tracks execution failures that are caused by connection problems.
It does so by catching exceptions on command execution

If a connection problem is detected the connection is closed.
This might for instance trying to execute a rollback for when the wrapper is used for a database connection

On a regular interval (RECONNECT_INTERVAL) the wrapper will try to restore the connection.
When the connection is restored, the failed command is re-executed
"""
import functools
from time import sleep

RECONNECT_INTERVAL = 60  # Duration in seconds to try to reconnect


class AutoReconnector:

    def __init__(self, is_connected, connect, disconnect):
        """Constructor

        Register the required functions to recognise and restore connection problems

        :param is_connected: Function that tells whether the connection is alive and working OK
        :param connect: Function to establish a connection
        :param disconnect: Function to close a connection
        """
        self.is_connected = is_connected
        self.connect = connect
        self.disconnect = disconnect

    def reconnect(self):
        """Reconnect

        First the connection is closed. This allows for cleanup the connection data and probably do some error recovery

        Program execution is then paused for RECONNECT_INTERVAL seconds

        After that, the connection is tried to re-establish

        If re-establishment fails the whole reconnect procedure is retried

        :return:
        """
        self.disconnect()
        print(f"Try to reconnect in {RECONNECT_INTERVAL} seconds...")
        sleep(RECONNECT_INTERVAL)
        if not self.connect():
            self.reconnect()  # Try again...

    def exec(self, func, *args, **kwargs):
        """Execute a method and catch any connection problems

        An optimistic approach is used.

        The function is executed
        Any exceptions are catched.
        If the exception is due to a connection problem, a reconnect is executed
        Once the connection is re-established the whole exec procedure is retried

        :param func: the function to execute
        :param args: function parameters
        :param kwargs: function parameters
        :return: the function result
        """
        result = None
        try:
            # Optimistic execution
            # Try to execute function, catch any exception
            result = func(*args, **kwargs)
        except Exception as e:
            # Check if the exception is due to a connection problem
            if not self.is_connected():
                # Report connection problem
                print("Connection problem, operation failed", str(e))
                self.reconnect()
                return self.exec(func, *args, **kwargs)  # Try again...
            else:
                # If not, re-raise the exception
                raise e
        return result


def auto_reconnect_wrapper(is_connected, connect, disconnect):
    """Auto reconnect wrapper

    This function returns a wrapper that can be used to protect functions against connection problems.

    In order to do this, the wrapper requires a couple of methods to control the connection

    :param is_connected: Function that tells whether the connection is alive and working OK
    :param connect: Function to establish a connection
    :param disconnect: Function to close a connection
    :return: A wrapper function
    """

    # Instantiate an auto reconnector object
    auto_reconnector = AutoReconnector(is_connected, connect, disconnect)

    # Use to auto reconnector to implement the wrapper
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            return auto_reconnector.exec(func, *args, **kwargs)

        return inner_wrapper

    return wrapper
