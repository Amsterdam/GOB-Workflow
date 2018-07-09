import time
import atexit

from workflow.config import MESSAGE_BROKER, QUEUES, WORKFLOW_QUEUE, LOG_QUEUE
from workflow.message_broker.async_message_broker import AsyncConnection

def on_message(queue, key, msg):
    if queue == LOG_QUEUE:
        print("Log", msg)
    elif queue == WORKFLOW_QUEUE:
        print("workflow", msg)
    else:
        print("Unknown message type received")


connection = AsyncConnection(MESSAGE_BROKER)

if connection.connect():
    atexit.register(connection.disconnect)

    while True:
        connection.subscribe(QUEUES, on_message)
        # Report some statistics or whatever is useful
        time.sleep(60)
