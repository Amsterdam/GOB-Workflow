"""Main workflow logic

The workflow manager subscribes to the workflow and log queues.

Log messages are simply printed (for now)
Workflow messages consist of proposals. A proposal is evaluated (for now always OK) and then routed as a request
to the service that can handle the proposal.

"""
import time

from gobworkflow.config import QUEUES, WORKFLOW_EXCHANGE, LOG_EXCHANGE, CONNECTION_PARAMS
from gobworkflow.message_broker.async_message_broker import AsyncConnection


# todo this has been made more generic in GOB-Upload, refactor here, move it to package
def on_message(connection, queue, key, msg):
    """Called on every message receipt

    :param queue: the message broker queue
    :param key: the identification of the message (e.g. fullimport.proposal)
    :param msg: the contents of the message
    :return:
    """

    if queue["name"] == LOG_EXCHANGE:
        # For now, print the message to stdout
        # Eventually store the log message in a database
        print(msg["msg"])
    elif queue["name"] == WORKFLOW_EXCHANGE:
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


with AsyncConnection(CONNECTION_PARAMS) as connection:

    # Subscribe to the queues
    connection.subscribe(QUEUES, on_message)

    # Repeat forever
    print("Workflow manager started")
    while True:
        time.sleep(60)
        # Report some statistics or whatever is useful
        print(".")
