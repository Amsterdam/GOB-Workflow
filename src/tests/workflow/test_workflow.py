import datetime

from unittest import TestCase, mock

from gobworkflow.workflow.config import START
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
        self.workflow._workflow_start = mock.MagicMock()
        self.workflow._workflow_end = mock.MagicMock()
        self.workflow._step_start = mock.MagicMock()
        self.workflow._step_end = mock.MagicMock()
        WORKFLOWS["Workflow"]["Step"]["function"] = mock.MagicMock()
        WORKFLOWS["Workflow"]["Next"]["function"] = mock.MagicMock()
        WORKFLOWS["Workflow"]["OtherNext"]["function"] = mock.MagicMock()

    def test_create(self):
        self.assertIsNotNone(self.workflow)

    def test_start(self):
        self.workflow.start({})

        WORKFLOWS["Workflow"]["Step"]["function"].assert_called_with({})
        self.workflow._workflow_start.assert_called()
        self.workflow._workflow_end.assert_not_called()
        self.workflow._step_start.assert_called()
        self.workflow._step_end.assert_not_called()

    def test_handle_result_without_next(self):
        handler = self.workflow.handle_result()
        handler({"condition": False})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        self.workflow._workflow_start.assert_not_called()
        self.workflow._workflow_end.assert_called()
        self.workflow._step_start.assert_not_called()
        self.workflow._step_end.assert_called()

    def test_handle_result_with_next(self):
        handler = self.workflow.handle_result()
        handler({"condition": True})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_called_with({"condition": True})
        self.workflow._workflow_start.assert_not_called()
        self.workflow._workflow_end.assert_not_called()
        self.workflow._step_start.assert_called()
        self.workflow._step_end.assert_called()

    def test_handle_result_with_multiple_nexts(self):
        handler = self.workflow.handle_result()
        handler({"next": True})

        # Only execute the first matching next step
        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        WORKFLOWS["Workflow"]["OtherNext"]["function"].assert_called_with({"next": True})

class TestJobManagement(TestCase):

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def setUp(self):
        self.workflow = Workflow("Workflow", "Step")

    def test_workflow_start(self):
        job = self.workflow._workflow_start({"a": 1, "b": "string", "c": True})
        self.assertEqual(job["name"], "Workflow.1.string.True")
        self.assertEqual(job["type"], "Workflow")
        self.assertEqual(job["args"], ["1", "string", "True"])
        self.assertIsInstance(job["start"], datetime.datetime)
        self.assertIsNone(job["end"])
        self.assertEqual(job["status"], "started")

    def test_workflow_end(self):
        job = self.workflow._workflow_end({"jobid": "any jobid"})
        self.assertEqual(job["id"], "any jobid")
        self.assertIsInstance(job["end"], datetime.datetime)
        self.assertEqual(job["status"], "ended")

    def test_step_start(self):
        step = self.workflow._step_start("any step")
        self.assertEqual(step["name"], "any step")
        self.assertIsInstance(step["start"], datetime.datetime)
        self.assertIsNone(step["end"])
        self.assertEqual(step["status"], "started")

    def test_step_end(self):
        step = self.workflow._step_end()
        self.assertEqual(step["name"], "Step")
        self.assertIsInstance(step["end"], datetime.datetime)
        self.assertEqual(step["status"], "ended")
