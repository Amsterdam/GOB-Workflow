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


@mock.patch("gobworkflow.workflow.workflow.WorkflowTreeNode")
class TestWorkflow(TestCase):

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def setUp(self):
        self.workflow = Workflow("Workflow", "Step")

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def test_create(self, mock_tree):
        self.assertIsNotNone(self.workflow)
        self.assertFalse(self.workflow._workflow_changed)

    @mock.patch("gobworkflow.workflow.workflow.Workflow._build_dynamic_workflow")
    def test_init_dynamic_workflow(self, mock_build_dynamic_workflow, mock_tree):
        wf = Workflow('Workflow', 'Step', 'dynamic workflow steps')
        mock_build_dynamic_workflow.assert_called_with('dynamic workflow steps')
        mock_tree.from_dict.assert_not_called()

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def test_init_changed_workflow(self, mock_tree):
        mock_tree.from_dict.return_value.get_node.return_value = None
        wf = Workflow('Workflow', 'Step')
        self.assertTrue(wf._workflow_changed)
        self.assertEqual(mock_tree.from_dict.return_value, wf._step)

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.logger")
    @mock.patch("gobworkflow.workflow.workflow.retry_workflow")
    def test_retry_or_fail(self, mock_retry_workflow, mock_logger, mock_tree):
        mock_tree.from_dict.return_value.get_node.return_value = None

        # Fail if no retry is specified
        wf = Workflow('Workflow', 'Step')
        msg = {}
        wf.retry_or_fail(msg, 0)
        mock_retry_workflow.assert_not_called()
        mock_logger.error.assert_called()
        mock_logger.error.reset_mock()

        # Create a workflow spec if this is missing
        wf.retry_or_fail(msg, 10)
        mock_retry_workflow.assert_called_with({
            'workflow': {
                'workflow_name': 'Workflow',
                'step_name': 'Step',
                'retry_time': 10}})
        mock_logger.error.assert_not_called()

        # Do not overwrite an existing workflow spec
        msg = {
            'workflow': 'my workflow'
        }
        wf.retry_or_fail(msg, 10)
        mock_retry_workflow.assert_called_with({'workflow': 'my workflow'})
        mock_logger.error.assert_not_called()

        # Fail if retry fails
        mock_retry_workflow.return_value = False
        wf.retry_or_fail(msg, 10)
        mock_logger.error.assert_called()

    DYNAMIC_WORKFLOWS = {
        'wf1': {
            START: 'wf1_step1',
            'wf1_step1': {
                'function': mock.MagicMock(),
                'next': [{'step': 'wf1_step2'}]
            },
            'wf1_step2': {
                'function': mock.MagicMock(),
            }
        },
        'wf2': {
            START: 'wf2_step1',
            'wf2_step1': {
                'function': mock.MagicMock(),
                'next': [{'step': 'wf2_step2'}]
            },
            'wf2_step2': {
                'function': mock.MagicMock(),
            }
        }
    }


    class MockNode:
        class MockLeaf:
            def __init__(self):
                self.appended = None

            def append_node(self, node):
                self.appended = node

        def __init__(self, workflow=None, name=None, function=None):
            self.workflow = workflow
            self.appended = None
            self.header_params = None
            self.leafs = [self.MockLeaf()]
            self.name = name
            self.function = function

        @classmethod
        def from_dict(cls, workflow):
            return cls(workflow)

        def append_to_names(self, s):
            self.appended = s

        def set_header_parameters(self, params):
            self.header_params = params

        def get_leafs(self):
            return self.leafs

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", DYNAMIC_WORKFLOWS)
    def test_build_dynamic_workflow(self, mock_tree):
        dynamic = [
            {
                'type': 'workflow',
                'workflow': 'wf1',
                'header': {
                    'attribute1': 'val1',
                    'attribute2': 'val2'
                }
            },
            {
                'type': 'workflow',
                'workflow': 'wf2',
                'header': {
                    'attribute3': 'val3',
                }
            },
        ]
        mock_tree.from_dict = self.MockNode.from_dict

        wf = Workflow('Workflow', dynamic_workflow_steps=dynamic)

        generated = wf._step
        self.assertEqual(self.DYNAMIC_WORKFLOWS['wf1'], generated.workflow)
        self.assertEqual('0', generated.appended)
        self.assertEqual({
            'attribute1': 'val1',
            'attribute2': 'val2',
        }, generated.header_params)

        next_wf = generated.leafs[0].appended
        self.assertEqual(self.DYNAMIC_WORKFLOWS['wf2'], next_wf.workflow)
        self.assertEqual('1', next_wf.appended)
        self.assertEqual({
            'attribute3': 'val3',
        }, next_wf.header_params)

    @mock.patch("gobworkflow.workflow.workflow.start_step")
    def test_build_dynamic_workflow_step(self, mock_start_step, mock_tree):
        dynamic = [
            {
                'type': 'workflow_step',
                'step_name': 'the_step',
                'header': {
                    'attribute1': 'val1',
                }
            }
        ]

        wf = Workflow('Workflow', dynamic_workflow_steps=dynamic)

        self.assertEqual(mock_tree.return_value, wf._step)

    def test_build_dynamic_workflow_notimplemented(self, mock_tree):
        dynamic = [
            {
                'type': 'non_existent',
            }
        ]
        with self.assertRaises(NotImplementedError):
            wf = Workflow('Workflow', dynamic_workflow_steps=dynamic)

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k, **kwargs: False)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        self.workflow._function = mock.MagicMock()
        self.workflow.start({})

        self.workflow._function.assert_called_with(self.workflow._step)
        self.workflow._function.return_value.assert_called_with({'header': {}})
        job_start.assert_called_with('Workflow', {'header': {}})

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
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
        }, 0)

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k, **kwargs: True)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_and_end_job_runs(self, job_start, step_start, mock_tree):
        self.workflow._function = mock.MagicMock()
        self.workflow.reject = mock.MagicMock()
        self.workflow.start({})
        self.workflow._function.assert_not_called()
        self.workflow.reject.assert_called_once()
        job_start.assert_called_with("Workflow", {'header': {'process_id': mock.ANY}})

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k, **kwargs: False)
    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_and_end(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        WORKFLOWS["Workflow"]["Step"]["function"] = lambda _ : END_OF_WORKFLOW
        self.workflow.start({})
        job_start.assert_called_with('Workflow', {'header': {}, 'summary': {}})
        step_start.assert_called_with('Step', {})

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.job_runs", lambda j, k, **kwargs: False)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_start")
    def test_start_with_contents(self, job_start, step_start, mock_tree):
        job_start.return_value = {'id': "Any process id"}
        self.workflow._function = mock.MagicMock()
        self.workflow.start({'summary': 'any summary', 'contents': []})

        self.workflow._function.return_value.assert_called_with({'summary': 'any summary', 'contents': [], 'header': {}})

        job_start.assert_called_with('Workflow', {'summary': 'any summary', 'contents': [], 'header': {}})

    @mock.patch("gobworkflow.workflow.workflow.job_update")
    def test_update_job_log_counts(self, mock_job_update, mock_tree):
        job = type('Job', (), {
            'id': 'jobid',
            'log_counts': {
                'data_warnings': 5,
                'data_errors': 2
            }
        })
        log_counts = {
            'data_warnings': 2,
            'data_info': 1
        }
        self.workflow._update_job_log_counts(job, log_counts)
        mock_job_update.assert_called_with({
            'id': 'jobid',
            'log_counts': {
                'data_warnings': 7,
                'data_errors': 2,
                'data_info': 1,
            }
        })

    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    @mock.patch("gobworkflow.workflow.workflow.job_get")
    def test_handle_result_without_next(self, job_get, job_end, mock_tree):
        self.workflow._update_job_log_counts = mock.MagicMock()
        self.workflow._function = mock.MagicMock()
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": False, "summary": {"log_counts": {"data_warnings": 5}}})

        self.workflow._function.assert_not_called()
        job_end.assert_called()
        self.workflow._update_job_log_counts.assert_called_with(job_get(), {"data_warnings": 5})

    @mock.patch("gobworkflow.workflow.workflow.logger")
    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.publish")
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    @mock.patch("gobworkflow.workflow.workflow.job_get")
    def test_handle_result_without_next_on_workflow_complete(self, job_get, job_end, mock_publish, mock_logger, mock_tree):
        self.workflow._update_job_log_counts = mock.MagicMock()
        self.workflow._function = mock.MagicMock()
        msg = {"header": {'on_workflow_complete': {'key': 'the key', 'exchange': 'the exchange'}}, "condition": False, "summary": {"log_counts": {"data_warnings": 5}}}
        handler = self.workflow.handle_result()
        handler(msg)

        self.workflow._function.assert_not_called()
        job_end.assert_called()
        self.workflow._update_job_log_counts.assert_called_with(job_get(), {"data_warnings": 5})
        mock_publish.assert_called_with('the exchange', 'the key', msg)

        # Test invalid dict
        mock_publish.reset_mock()
        msg['header']['on_workflow_complete'] = {}
        handler(msg)
        mock_publish.assert_not_called()
        mock_logger.error.assert_called_once_with("on_workflow_complete should be a dict with keys 'exchange' and 'key'")

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.job_get")
    def test_handle_result_with_next(self, job_get, step_start, mock_tree):
        self.workflow._update_job_log_counts = mock.MagicMock()
        handler = self.workflow.handle_result()
        handler({"header": {}, "condition": True})

        WORKFLOWS["Workflow"]["Next"]["function"].assert_called_with({"header": {}, "summary": {}, "condition": True})
        step_start.assert_called()

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.step_start", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.job_get")
    def test_handle_result_with_multiple_nexts(self, job_get, mock_tree):
        self.workflow._update_job_log_counts = mock.MagicMock()
        handler = self.workflow.handle_result()
        handler({"header": {}, "next": True})

        # Only execute the first matching next step
        WORKFLOWS["Workflow"]["Next"]["function"].assert_not_called()
        WORKFLOWS["Workflow"]["OtherNext"]["function"].assert_called_with({"header": {}, "summary": {}, "next": True})

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.logger", mock.MagicMock())
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    @mock.patch("gobworkflow.workflow.workflow.step_status")
    @mock.patch("gobworkflow.workflow.workflow.job_end")
    def test_reject(self, mock_job_end, mock_step_status, mock_step_start, mock_tree):
        mock_step_start.return_value = {
            'id': 'stepid',
        }

        self.assertEqual(mock_job_end.return_value, self.workflow.reject({'header': {}}, {'id': 'jobid'}))
        mock_step_start.assert_called_with('accept', {
            'process_id': 'jobid',
            'entity': None
        })
        mock_step_status.assert_has_calls([
            mock.call('jobid', 'stepid', 'started'),
            mock.call('jobid', 'stepid', 'rejected'),
        ])
        mock_job_end.assert_called_with('jobid', 'rejected')

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    @mock.patch("gobworkflow.workflow.workflow.step_start")
    def test_function_end(self, mock_step_start, mock_tree):
        step = mock.MagicMock()
        step.function.return_value = END_OF_WORKFLOW
        self.workflow.end_of_workflow = mock.MagicMock()

        msg = {
            'header': {'the': 'header'},
            'summary': {}
        }

        func = self.workflow._function(step)
        func(msg)
        self.workflow.end_of_workflow.assert_called_with(msg)

    @mock.patch("gobworkflow.workflow.workflow.WORKFLOWS", WORKFLOWS)
    def test_handle_result_changed_workflow(self, mock_tree):
        # When _workflow_changed is set to True, handle_result should run _step instead of the next step
        wf = Workflow('Workflow', 'Step')
        wf._workflow_changed = True
        wf._step = mock.MagicMock()
        wf._function = mock.MagicMock()
        msg = mock.MagicMock()

        handle_msg_func = wf.handle_result()
        self.assertEqual(wf._function.return_value.return_value, handle_msg_func(msg))
        wf._function.assert_called_with(wf._step)
        wf._function.return_value.assert_called_with(msg)
