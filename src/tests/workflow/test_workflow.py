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
@mock.patch("gobworkflow.workflow.workflow.WorkflowTreeNode")
class TestWorkflow(TestCase):

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def setUp(self):
        self.workflow = Workflow("Workflow", "Step")

    def test_create(self, mock_tree):
        self.assertIsNotNone(self.workflow)

    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k: False)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        self.workflow._function = mock.MagicMock()
        self.workflow.start({})

        self.workflow._function.assert_called_with(self.workflow._step)
        self.workflow._function.return_value.assert_called_with({'header': {'process_id': 'Any process id'}})
        job_start.assert_called_with('Workflow', {'header': {'process_id': 'Any process id'}})

    def test_start_new(self, mock_tree):
        self.workflow.start = mock.MagicMock()
        attrs = {
            'h1': 'v1',
            'h2': 'v2',
        }
        self.workflow.start_new(attrs)

        self.workflow.start.assert_called_with({
            'header': {
                'h1': 'v1',
                'h2': 'v2',
            }
        })

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k: True)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_and_end_job_runs(self, job_start, step_start, mock_tree):
        self.workflow._function = mock.MagicMock()
        self.workflow.reject = mock.MagicMock()
        self.workflow.start({})
        self.workflow._function.assert_not_called()
        self.workflow.reject.assert_called_once()
        job_start.assert_called_with("Workflow", {'header': {'process_id': mock.ANY}})

    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k: False)
    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_and_end(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        WORKFLOWS["Workflow"]["Step"]["function"] = lambda _ : END_OF_WORKFLOW
        self.workflow.start({})
        job_start.assert_called_with('Workflow', {'header': {'process_id': 'Any process id'}, 'summary': {}})
        step_start.assert_called_with('Step', {'process_id': 'Any process id'})

    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k: False)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_with_contents(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        self.workflow._function = mock.MagicMock()
        self.workflow.start({'summary': 'any summary', 'contents': []})

        self.workflow._function.return_value.assert_called_with({'summary': 'any summary', 'contents': [], 'header': {'process_id': 'Any process id'}})

        job_start.assert_called_with('Workflow', {'summary': 'any summary', 'contents': [], 'header': {'process_id': 'Any process id'}})

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    def test_handle_result_without_next(self, job_end, mock_tree):
        self.workflow._function = mock.MagicMock()
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": False})

        self.workflow._function.assert_not_called()
        job_end.assert_called()

    @mock.patch("gobworkflow.workflow.workflow.step_start")
    def test_handle_result_with_next(self, step_start, mock_tree):
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": True})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_called_with({"header": {}, "summary": {}, "condition": True})
        step_start.assert_called()

    @mock.patch("gobworkflow.workflow.workflow.step_start", mock.MagicMock())
    def test_handle_result_with_multiple_nexts(self, mock_tree):
        handler = self.workflow.handle_result()
        handler({"header": {}, "next": True})

        # Only execute the first matching next step
        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        WORKFLOWS["Workflow"]["OtherNext"]["function"].assert_called_with({"header": {}, "summary": {}, "next": True})

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.step_status")
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    def test_reject(self, mock_job_end, mock_step_status, mock_step_start, mock_tree):
        mock_step_start.return_value = {
            'id': 'stepid',
        }

        self.assertEqual(mock_job_end.return_value, self.workflow.reject('action', {'header': {}}, {'id': 'jobid'}))
        mock_step_start.assert_called_with('accept', {
            'process_id': 'jobid',
            'entity': None
        })
        mock_step_status.assert_has_calls([
            mock.call('jobid', 'stepid', 'started'),
            mock.call('jobid', 'stepid', 'rejected'),
        ])
        mock_job_end.assert_called_with('jobid', 'rejected')

    @mock.patch("gobworkflow.workflow.workflow.step_start")
    def test_function_end(self, mock_step_start, mock_tree):
        step = mock.MagicMock()
        step.function.return_value = END_OF_WORKFLOW
        self.workflow.end_of_workflow = mock.MagicMock()

        msg = {
            'header': 'the header',
            'summary': {}
        }

        func = self.workflow._function(step)
        func(msg)
        self.workflow.end_of_workflow.assert_called_with(msg)
