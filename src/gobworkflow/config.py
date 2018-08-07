import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_QUEUE = "gob.workflow.proposal"
LOG_QUEUE = "gob.log.all"

QUEUES = [
    {
        "exchange": "gob.workflow",
        "name": WORKFLOW_QUEUE,
        "key": "*.proposal"
    },
    {
        "exchange": "gob.log",
        "name": LOG_QUEUE,
        "key": "#"
    }
]
