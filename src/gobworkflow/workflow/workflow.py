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

from gobworkflow.workflow.config import WORKFLOWS, START, DEFAULT_CONDITION
from gobworkflow.workflow.jobs import job_start, job_end, step_start
from gobworkflow.workflow.start import END_OF_WORKFLOW


class Workflow():

    def __init__(self, workflow_name, step_name=None):
        """
        Initializes a workflow.

        Use to workflow name and step name to find the workflow and step in the WORKFLOWS

        If no step name is specified, the step is set to the start step of the workflow

        :param workflow_name: Name of the workflow
        :param step_name: Name of the step within the workflow, default: start step
        """
        self._workflow_name = workflow_name
        self._workflow = WORKFLOWS[self._workflow_name]

        self._step_name = self._workflow[START] if step_name is None else step_name
        self._step = self._workflow[self._step_name]

    def start_new(self, header_attrs: dict):
        self.start({'header': {**header_attrs}})

    def start(self, msg):
        """
        Start a workflow
        :param msg: The parameters to the workflow
        :return:
        """
        if not msg.get("header"):
            msg["header"] = {}
        job_start(self._workflow_name, msg)
        self._function(self._step_name)(msg)

    def _end_of_workflow(self, msg):
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
            next = [next for next in self._next_steps() if self._condition(next)(msg)]
            if next:
                # Execute the first one that matches
                self._function(next[0]["step"])(msg)
            else:
                # No next => end of workflow reached
                self._end_of_workflow(msg)

        return handle_msg

    def _function(self, step_name):
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
            step_start(step_name, msg["header"])  # Explicit start of new step
            # Clear any summary from the previous step
            msg['summary'] = {}
            result = self._workflow[step_name].get("function", lambda _: None)(msg)
            if result == END_OF_WORKFLOW:
                self._end_of_workflow(msg)

        return exec_step

    def _next_steps(self):
        """
        Get the next steps for the current workflow step
        :return: The next steps, or an empty list when no next steps exists (end of workflow)
        """
        return self._step.get("next", [])

    def _condition(self, next):
        """
        Get the function that tests whether a next step is eligible for execution given the current result (msg)

        Default a next step matches unconditionally
        :param next: A next step config
        :return:
        """
        return next.get("condition", DEFAULT_CONDITION)
