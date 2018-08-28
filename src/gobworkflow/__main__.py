"""Main workflow logic

The workflow manager subscribes to the workflow and log queues.

Log messages are simply printed (for now)
Workflow messages consist of proposals. A proposal is evaluated (for now always OK) and then routed as a request
to the service that can handle the proposal.

"""
from gobcore.message_broker.messagedriven_service import messagedriven_service

SERVICEDEFINITION = {
    'fullimport.proposal': {
        'queue': "gob.workflow.proposal",
        # for now only pass through the message-content:
        'handler': lambda msg: msg,
        'report_back': 'fullimport.request',
        'report_queue': 'gob.workflow.request'
    },
    'fullupdate.proposal': {
        'queue': "gob.workflow.proposal",
        # for now only pass through the message-content:
        'handler': lambda msg: msg,
        'report_back': 'fullupdate.request',
        'report_queue': 'gob.workflow.request'
    },
}

messagedriven_service(SERVICEDEFINITION)
