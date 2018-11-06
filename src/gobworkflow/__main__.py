"""Main workflow logic

The workflow manager subscribes to the workflow and log queues.

Log messages are simply printed (for now)
Workflow messages consist of proposals. A proposal is evaluated (for now always OK) and then routed as a request
to the service that can handle the proposal.

"""
from gobcore.message_broker.config import LOG_EXCHANGE, STATUS_EXCHANGE, HEARTBEAT_QUEUE, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.log import get_logger

from gobworkflow.storage.storage import connect, save_log
from gobworkflow.heartbeats import on_heartbeat


logger = get_logger(name="WORKFLOW")


def pass_through(msg, type):
    log_msg = ""
    if type == 'import':
        log_msg = "Import proposal accepted"
    elif type == 'update':
        log_msg = "Update proposal accepted"

    extra_log_kwargs = {
        'process_id': msg['header']['process_id'],
        'source': msg['header']['source'],
        'entity': msg['header']['entity']
    }

    logger.info(log_msg, extra=extra_log_kwargs)
    return msg


SERVICEDEFINITION = {
    'import_proposal': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.proposal',
        'key': 'fullimport.proposal',
        # for now only pass through the message-content:
        'handler': lambda msg: pass_through(msg, 'import'),
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
        'handler': lambda msg: pass_through(msg, 'update'),
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
    'heartbeat_monitor': {
        'exchange': STATUS_EXCHANGE,
        'queue': HEARTBEAT_QUEUE,
        'key': 'HEARTBEAT',
        'handler': on_heartbeat,
    },
}

connect()
messagedriven_service(SERVICEDEFINITION, "Workflow")
