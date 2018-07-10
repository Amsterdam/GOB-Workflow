import time
import atexit

from workflow.config import MESSAGE_BROKER, QUEUES, WORKFLOW_QUEUE, LOG_QUEUE
from workflow.message_broker.async_message_broker import AsyncConnection


def on_message(queue, key, msg):
    """Called on every message receipt

    :param queue: the message broker queue
    :param key: the identification of the message (e.g. fullimport.proposal)
    :param msg: the contents of the message
    :return:
    """
    if queue == LOG_QUEUE:
        # Eventually store the log message in a database
        print("Log", msg)
    elif queue == WORKFLOW_QUEUE:
        # Eventually route message to next component
        # fullimport.proposal => fullimport.request (compare against current data)
        # fullupdate.proposal => fullupdate.request (update current data)
        # fullupdate.completed (update process finished)
        print("Workflow", msg)
    else:
        # This should never happen, the subscription routing_key should filter the messages
        print("Unknown message type received")
        return False  # Do not acknowledge the message

    return True  # Acknowledge message when it has been fully handled


# Construct a asynchronous connection with the message broker
connection = AsyncConnection(MESSAGE_BROKER)

# Try to connect
if connection.connect():

    # Schedule a disconnect to gracefully end the connection
    atexit.register(connection.disconnect)

    # Subscribe to the required message queues
    connection.subscribe(QUEUES, on_message)

    # Repeat forever
    while True:
        # Report some statistics or whatever is useful
        time.sleep(60)
