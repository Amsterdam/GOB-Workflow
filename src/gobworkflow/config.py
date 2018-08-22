"""Workflow configuration

Definition of the message broker queues, exchanges and routing keys.

Two main queues are defined:
    WORKFLOW - the normal import-upload traffic.
               Proposals (units of work) are received and routed by the workflow manager.
    LOG - the logging output of each GOB process is routed over this queue

"""
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
