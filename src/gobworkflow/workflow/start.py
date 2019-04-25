"""
Module containing workflow start methods.

Starting a next step may require that a result message contains no errors.
A has_no_errors method is available to be used as a default condition to start a next step
"""
from gobcore.logging.logger import logger
from gobcore.message_broker.config import REQUEST_QUEUE
from gobcore.message_broker import publish


def start_workflows(workflow_name, step_name, msg):
    """
    Start workflows at the given step with the specified message

    For each element in the msg['contents'] a workflow is started

    :param workflow_name: Refers to WORKFLOWS[workflow_name]
    :param step_name: Refers to WORKFLOWS[workflow_name][step_name]
    :param msg: The message that holds the info to start the workflow
    :return: None
    """
    # Contents is an array of contents. For each element the specified workflow is started
    for content in msg['contents']:
        # Construct the message from the given message, the contents is retrieved from the input message contents
        new_msg = {
            **msg,
            'contents': content
        }
        start_workflow(workflow_name, step_name, new_msg)


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


def start_step(key, msg):
    publish(REQUEST_QUEUE, f"{key}.start", msg)


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
