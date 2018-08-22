import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_EXCHANGE = "gob.workflow"
LOG_EXCHANGE = "gob.log"

QUEUES = [
    {
        "exchange": WORKFLOW_EXCHANGE,
        "name": WORKFLOW_EXCHANGE + '.proposal',
        "key": "*.proposal"
    },
    {
        "exchange": WORKFLOW_EXCHANGE,
        "name": WORKFLOW_EXCHANGE + '.request',
        "key": "*.request"
    },
    {
        "exchange": LOG_EXCHANGE,
        "name": LOG_EXCHANGE + '.all',
        "key": "#"
    }
]
