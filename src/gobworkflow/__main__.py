"""Main workflow logic

The workflow manager subscribes to the workflow and log queues.

Log messages are simply printed (for now)
Workflow messages consist of proposals. A proposal is evaluated (for now always OK) and then routed as a request
to the service that can handle the proposal.

"""
from gobcore.message_broker.config import LOG_EXCHANGE, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobworkflow.storage import connect, save_log

SERVICEDEFINITION = {
    'import_proposal': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.proposal',
        'key': 'fullimport.proposal',
        # for now only pass through the message-content:
        'handler': lambda msg: msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': 'gob.workflow.request',
            'key': 'fullimport.request'
        }
    },
    'update_proposal': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.proposal',
        'key': 'fullupdate.proposal',
        # for now only pass through the message-content:
        'handler': lambda msg: msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': 'gob.workflow.request',
            'key': 'fullupdate.request'
        }
    },
    'save_logs': {
        'exchange': LOG_EXCHANGE,
        'queue': 'gob.log.all',
        'key': '#',
        'handler': save_log,
    },
}

connect()
messagedriven_service(SERVICEDEFINITION)
