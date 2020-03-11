"""Workflow

Workflow interprets the WORKFLOWS that are defined in config

Workflows are asynchronous and implemented by using the message broker.

A workflow is:
- started
- continued
- ended

A workflow step is, once started, continued by handling the result of the step
The result is received from the message broker (which ends the step)
The result is interpreted by the rules of the workflow
If a next step is found then this step is started
If not, the workflow is ended
"""
from gobcore.logging.logger import logger

from gobworkflow.workflow.config import WORKFLOWS
from gobworkflow.workflow.jobs import job_start, job_end, step_start, step_status
from gobcore.status.heartbeat import STATUS_START, STATUS_REJECTED
from gobworkflow.storage.storage import job_runs
from gobworkflow.workflow.start import END_OF_WORKFLOW

from gobworkflow.workflow.tree import WorkflowTreeNode


class Workflow:

    def __init__(self, workflow_name, step_name=None):
        """
        Initializes a workflow.

        Use to workflow name and step name to find the workflow and step in the WORKFLOWS

        If no step name is specified, the step is set to the start step of the workflow

        :param workflow_name: Name of the workflow
        :param step_name: Name of the step within the workflow, default: start step
        """
        self._workflow_name = workflow_name
        workflow = WorkflowTreeNode.from_dict(WORKFLOWS[self._workflow_name])

        self._step = workflow if step_name is None else workflow.get_node(step_name)

    def start_new(self, header_attrs: dict):
        return self.start({'header': {**header_attrs}})

    def start(self, msg):
        """
        Start a workflow
        :param msg: The parameters to the workflow
        :return:
        """
        job = None
        job_id = msg.get("header", {}).get("jobid")
        if job_id is None:
            job = job_start(self._workflow_name, msg)
            msg['header'] = {
                **msg.get('header', {}),
                'process_id': job['id']
            }
            if job_runs(job, msg):
                return self.reject(self._workflow_name, msg, job)
        self._function(self._step)(msg)
        return job

    def reject(self, action, msg, job):
        """
        Reject a message because the job is already active within GOB

        :param msg:
        :param job:
        :return:
        """
        # Start a workflow step to reject the message
        msg["header"]["process_id"] = job['id']
        msg["header"]["entity"] = msg["header"].get('collection')
        step = step_start("accept", msg['header'])
        step_status(job['id'], step['id'], STATUS_START)
        logger.configure(msg, action.upper())
        logger.error(f"Job {action} start rejected, job is already active")
        # End the workflow step and then the workflow job
        step_status(job['id'], step['id'], STATUS_REJECTED)
        return job_end(job['id'], STATUS_REJECTED)

    @classmethod
    def end_of_workflow(self, msg):
        logger.configure(msg, "WORKFLOW")
        logger.info(f"End of workflow")
        job_end(msg["header"].get("jobid"))

    def handle_result(self):
        """
        Get a handler that processes the result of a workflow step

        Either a next step is found and executed
        Or the workflow is ended

        When multiple next steps are found, only the first one is executed
        :return:
        """
        def handle_msg(msg):
            """
            Handle the result of a step
            :param msg: The results of the step that was executed
            :return:
            """
            next = [next for next in self._step.next if next.condition(msg)]
            if next:
                # Execute the first one that matches
                self._function(next[0].node)(msg)
            else:
                # No next => end of workflow reached
                self.end_of_workflow(msg)

        return handle_msg

    def _function(self, step: WorkflowTreeNode):
        """
        Get the function that is to be executed for the workflow step with the given name
        :param step_name: The name of the step within the workflow
        :return:
        """
        def exec_step(msg):
            """
            Execute a workflow step
            :param msg: Workflow step parameters
            :return:
            """
            step_start(step.name, msg["header"])  # Explicit start of new step
            # Clear any summary from the previous step
            msg['summary'] = {}
            result = step.function(msg)
            if result == END_OF_WORKFLOW:
                self.end_of_workflow(msg)

        return exec_step
