"""Main workflow logic

The workflow manager subscribes to the workflow and log queues.

Log messages are simply printed (for now)
Workflow messages consist of proposals. A proposal is evaluated (for now always OK) and then routed as a request
to the service that can handle the proposal.

"""
from gobcore.message_broker.config import LOG_EXCHANGE, STATUS_EXCHANGE, HEARTBEAT_QUEUE, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobworkflow.storage.storage import connect, save_log
from gobworkflow.workflow.workflow import Workflow
from gobworkflow.heartbeats import on_heartbeat

from gobworkflow.workflow.config import IMPORT, IMPORT_READ, IMPORT_COMPARE


SERVICEDEFINITION = {
    'import_proposal': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.proposal',
        'key': 'fullimport.proposal',
        'handler': Workflow(IMPORT, IMPORT_READ).handle_result()
    },
    'update_proposal': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.proposal',
        'key': 'fullupdate.proposal',
        'handler': Workflow(IMPORT, IMPORT_COMPARE).handle_result()
    },
    'save_logs': {
        'exchange': LOG_EXCHANGE,
        'queue': 'gob.log.all',
        'key': '#',
        'handler': save_log
    },
    'heartbeat_monitor': {
        'exchange': STATUS_EXCHANGE,
        'queue': HEARTBEAT_QUEUE,
        'key': 'HEARTBEAT',
        'handler': on_heartbeat
    },
}

connect()
messagedriven_service(SERVICEDEFINITION, "Workflow")
