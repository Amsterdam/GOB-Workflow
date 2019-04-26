import datetime

from unittest import TestCase, mock

from gobworkflow.workflow.config import START
from gobworkflow.workflow.start import END_OF_WORKFLOW
from gobworkflow.workflow.workflow import Workflow

WORKFLOWS = {
    "Workflow": {
        START: "Step",
        "Step": {
            "function": mock.MagicMock(),
            "next": [
                {
                    "condition": lambda msg: msg.get("condition", False),
                    "step": "Next"
                },
                {
                    "condition": lambda msg: msg.get("next", False),
                    "step": "OtherNext"
                },
                {
                    "condition": lambda msg: msg.get("next", False),
                    "step": "Next"
                },
            ],
        },
        "Next": {
            "function": mock.MagicMock(),
        },
        "OtherNext": {
            "function": mock.MagicMock(),
        }
    }
}

@mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
class TestWorkflow(TestCase):

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def setUp(self):
        self.workflow = Workflow("Workflow", "Step")
        WORKFLOWS["Workflow"]["Step"]["function"] = mock.MagicMock()
        WORKFLOWS["Workflow"]["Next"]["function"] = mock.MagicMock()
        WORKFLOWS["Workflow"]["OtherNext"]["function"] = mock.MagicMock()

    def test_create(self):
        self.assertIsNotNone(self.workflow)

    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start(self, job_start, step_start):
        self.workflow.start({})

        WORKFLOWS["Workflow"]["Step"]["function"].assert_called_with({"header": {}, "summary": {}})
        job_start.assert_called_with("Workflow", {"header": {}, "summary": {}})
        step_start.assert_called_with("Step", {})

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.step_end")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_and_end(self, job_start, step_end, step_start):
        WORKFLOWS["Workflow"]["Step"]["function"] = lambda _ : END_OF_WORKFLOW
        self.workflow.start({})
        job_start.assert_called_with("Workflow", {"header": {}, "summary": {}})
        step_start.assert_called_with("Step", {})
        step_end.assert_called_with({})

    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_with_contents(self, job_start, step_start):
        self.workflow.start({'summary': 'any summary', 'contents': []})

        WORKFLOWS["Workflow"]["Step"]["function"].assert_called_with({"header": {}, 'summary': {}, 'contents': []})
        job_start.assert_called_with("Workflow", {"header": {}, 'summary': {}, 'contents': []})
        step_start.assert_called_with("Step", {})

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_end")
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    def test_handle_result_without_next(self, job_end, step_end):
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": False})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        job_end.assert_called()
        step_end.assert_called()

    @mock.patch("gobworkflow.workflow.workflow.step_end")
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    def test_handle_result_with_next(self, step_start, step_end):
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": True})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_called_with({"header": {}, "summary": {}, "condition": True})
        step_start.assert_called()
        step_end.assert_called()

    @mock.patch("gobworkflow.workflow.workflow.step_end", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start", mock.MagicMock())
    def test_handle_result_with_multiple_nexts(self):
        handler = self.workflow.handle_result()
        handler({"header": {}, "next": True})

        # Only execute the first matching next step
        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        WORKFLOWS["Workflow"]["OtherNext"]["function"].assert_called_with({"header": {}, "summary": {}, "next": True})
