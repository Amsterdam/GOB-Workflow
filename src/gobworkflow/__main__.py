import time

from gobworkflow.config import MESSAGE_BROKER, QUEUES, WORKFLOW_QUEUE, LOG_QUEUE
from gobworkflow.message_broker.async_message_broker import AsyncConnection


def on_message(connection, queue, key, msg):
    """Called on every message receipt

    :param queue: the message broker queue
    :param key: the identification of the message (e.g. fullimport.proposal)
    :param msg: the contents of the message
    :return:
    """

    if queue["name"] == LOG_QUEUE:
        # For now, print the message to stdout
        # Eventually store the log message in a database
        print(msg["msg"])
    elif queue["name"] == WORKFLOW_QUEUE:
        if key == "fullimport.proposal":
            print("=> 'fullimport.proposal' accepted, publish 'fullimport.request'")
            connection.publish(queue, "fullimport.request", msg)
        elif key == "fullupdate.proposal":
            print("=> 'fullupdate.proposal' accepted, publish 'fullupdate.request'")
            connection.publish(queue, "fullupdate.request", msg)
        else:
            print("Unknown workflow message received", key)
            return False  # ignore message, leave for someone else
    else:
        # This should never happen...
        print("Unknown message type received", queue["name"], key)
        return False  # Do not acknowledge the message

    return True  # Acknowledge message when it has been fully handled


with AsyncConnection(MESSAGE_BROKER) as connection:

    # Subscribe to the queues
    connection.subscribe(QUEUES, on_message)

    # Repeat forever
    print("Workflow manager started")
    while True:
        time.sleep(60)
        # Report some statistics or whatever is useful
        print(".")