import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_QUEUE = "gob.workflow"
LOG_QUEUE = "gob.log"

QUEUES = [
    {
        "name": WORKFLOW_QUEUE,
        "key": "#"
    },
    {
        "name": LOG_QUEUE,
        "key": "#"
    }
]
