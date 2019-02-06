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
import datetime

from gobworkflow.workflow.config import WORKFLOWS, START


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

    def start(self, msg):
        """
        Start a workflow
        :param msg: The parameters to the workflow
        :return:
        """
        self._workflow_start(msg)
        self._function(self._step_name)(msg)

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
            self._step_end()  # On handle result the current step has ended
            next = [next for next in self._next_steps() if self._condition(next)(msg)]
            if next:
                # Execute the first one that matches
                self._function(next[0]["step"])(msg)
            else:
                # No next => end of workflow reached
                self._workflow_end(msg)

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
            self._step_start(step_name)  # Explicit start of new step
            self._workflow[step_name].get("function", lambda _: None)(msg)

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
        return next.get("condition", lambda _: True)

    def _timestamp(self):
        """
        Job and job steps are given UTC timestamps
        :return: The current UTC date time
        """
        return datetime.datetime.utcnow()

    def _workflow_start(self, msg):
        """
        Start a workflow

        Assign a name and register the parameters, including the start time
        :param msg: The workflow start parameters
        :return:
        """
        timestamp = self._timestamp()
        args = [str(val) for val in msg.values()]
        job = {
            "name": f"{self._workflow_name}.{'.'.join(args)}",
            "type": self._workflow_name,
            "args": args,
            "start": timestamp,
            "end": None,
            "status": "started"
        }
        return job

    def _workflow_end(self, msg):
        """
        End a workflow

        Register the end time and the status
        :param msg: The message that ended the workflow
        :return:
        """
        timestamp = self._timestamp()
        job = {
            "id": msg.get("jobid"),
            "end": timestamp,
            "status": "ended"
        }
        return job

    def _step_start(self, step_name):
        """
        Start a workflow step

        Register its name, mark it as started and register the start time
        :param step_name: The name of the step
        :return:
        """
        timestamp = self._timestamp()
        step = {
            "name": step_name,
            "start": timestamp,
            "end": None,
            "status": "started"
        }
        return step

    def _step_end(self):
        """
        End a workflow step

        Register its status and end time
        """
        timestamp = self._timestamp()
        step = {
            "name": self._step_name,
            "end": timestamp,
            "status": "ended"
        }
        return step
