"""Workflow configuration

Definition of the message broker queues, exchanges and routing keys.

Two main queues are defined:
    WORKFLOW - the normal import-upload traffic.
               Proposals (units of work) are received and routed by the workflow manager.
    LOG - the logging output of each GOB process is routed over this queue

"""
import os

import pika

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER,
                                      password=MESSAGE_BROKER_PASSWORD)
)

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
