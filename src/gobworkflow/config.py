import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_QUEUE = "gob.workflow"
LOG_QUEUE = "gob.log"

QUEUES = [
    {
        "exchange": "gob.workflow",
        "name": WORKFLOW_QUEUE+'.proposal',
        "key": "*.proposal"
    },
    {
        "exchange": "gob.workflow",
        "name": WORKFLOW_QUEUE+'.request',
        "key": "*.request"
    },
    {
        "exchange": "gob.log",
        "name": LOG_QUEUE+'.all',
        "key": "#"
    }
]
