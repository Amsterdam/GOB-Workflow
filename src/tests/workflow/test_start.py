from unittest import TestCase, mock

from gobcore.message_broker.config import WORKFLOW_EXCHANGE
from gobworkflow.workflow import start

class TestStart(TestCase):

    def setUp(self):
        pass

    @mock.patch('gobworkflow.workflow.start.publish')
    def testStartStep(self, mock_publish):
        msg = "any message"
        start.start_step("step", msg)
        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, "step.request", msg)

    @mock.patch('gobworkflow.workflow.start.publish')
    def testStartWorkflow(self, mock_publish):
        msg = {}
        workflow = "any workflow"
        step = "any step"
        start.start_workflow(workflow, step, msg)
        expect_msg = {
            'workflow': {
                'workflow_name': workflow,
                'step_name': step
            }
        }
        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, "workflow.request", expect_msg)

    @mock.patch('gobworkflow.workflow.start.step_status', mock.MagicMock())
    @mock.patch('gobworkflow.workflow.start.publish')
    def testStartWorkflows(self, mock_publish):
        workflow = "any workflow"
        step = "any step"

        msg = {
            'header': { 'jobid': 0, 'stepid': 0 },
            'contents': []
        }
        start.start_workflows(workflow, step, msg)
        mock_publish.assert_not_called()

        msg = {
            'header': { 'jobid': 0, 'stepid': 0 },
            'contents': [{'extra': 'content'}, {'extra': 'content'}]
        }
        start.start_workflows(workflow, step, msg)
        for content in msg['contents']:
            expect_msg = {
                **msg,
                'workflow': {
                    'workflow_name': workflow,
                    'step_name': step
                }
            }
            expect_msg['header'].update(content)
            mock_publish.assert_any_call(WORKFLOW_EXCHANGE, "workflow.request", expect_msg)
        self.assertEqual(mock_publish.call_count, len(msg['contents']))

    @mock.patch('gobworkflow.workflow.start.logger', mock.MagicMock())
    def testHasNoErrors(self):
        self.assertTrue(start.has_no_errors({}))
        self.assertTrue(start.has_no_errors({'summary': {}}))
        self.assertTrue(start.has_no_errors({'summary': {'errors': []}}))
        self.assertFalse(start.has_no_errors({'summary': {'errors': ['any error']}}))
