"""
Module containing workflow start methods.

Starting a next step may require that a result message contains no errors.
A has_no_errors method is available to be used as a default condition to start a next step
"""
import json

from gobcore.logging.logger import logger
from gobcore.message_broker.config import WORKFLOW_EXCHANGE
from gobcore.message_broker import publish
from gobcore.message_broker.offline_contents import load_message
from gobcore.status.heartbeat import STATUS_START, STATUS_OK

from gobworkflow.workflow.jobs import step_status

# Special return value that a function can return to end the current workflow
END_OF_WORKFLOW = "END_OF_WORKFLOW"


def start_workflows(workflow_name, step_name, msg):
    """
    Start workflows at the given step with the specified message

    For each element in the msg['contents'] a workflow is started

    :param workflow_name: Refers to WORKFLOWS[workflow_name]
    :param step_name: Refers to WORKFLOWS[workflow_name][step_name]
    :param msg: The message that holds the info to start the workflow
    :return: None
    """
    header = msg['header']
    jobid = header['jobid']
    stepid = header['stepid']

    step_status(jobid, stepid, STATUS_START)

    # Incoming message may be large. Manually load message from file if necessary
    msg, _ = load_message(msg, json.loads, {'stream_contents': False})

    # Contents is an array of contents. For each element the specified workflow is started
    for content in msg['contents']:
        # Construct the message from the given message, the contents is retrieved from the input message contents
        new_msg = {
            **msg,
            'contents': content
        }
        start_workflow(workflow_name, step_name, new_msg)

    step_status(jobid, stepid, STATUS_OK)
    return END_OF_WORKFLOW  # End current workflow when starting a new workflow


def start_workflow(workflow_name, step_name, msg):
    """
    Start workflow at the given step with the specified message

    :param workflow_name: Refers to WORKFLOWS[workflow_name]
    :param step_name: Refers to WORKFLOWS[workflow_name][step_name]
    :param msg: The message that holds the info to start the workflow
    :return: None
    """
    # Add the workflow info to the message
    msg['workflow'] = {
        'workflow_name': workflow_name,
        'step_name': step_name
    }
    # Post a message to start the specified workflow
    start_step("workflow", msg)
    return END_OF_WORKFLOW  # End current workflow when starting a new workflow


def start_step(key, msg):
    publish(WORKFLOW_EXCHANGE, f"{key}.request", msg)


def has_no_errors(msg):
    """
    Checks the message

    Interprets the message info and either return True to signal that the message was OK
    or return False and logs an error message explaining why the result was rejected
    :param msg: The message to check
    :return: True if the message is OK to proceed to the next step
    """
    summary = msg.get('summary')
    is_ok = True
    if summary:
        num_errors = len(summary.get('errors', []))
        is_ok = num_errors == 0
        if not is_ok:
            logger.configure(msg, "WORKFLOW")
            logger.warning(f"Workflow stopped because of {num_errors} error{'s' if num_errors > 1 else '' }")
    return is_ok
