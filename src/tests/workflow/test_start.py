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

    @mock.patch('gobworkflow.workflow.start.logger', mock.MagicMock())
    def testHasNoErrors(self):
        self.assertTrue(start.has_no_errors({}))
        self.assertTrue(start.has_no_errors({'summary': {}}))
        self.assertTrue(start.has_no_errors({'summary': {'errors': []}}))
        self.assertFalse(start.has_no_errors({'summary': {'errors': ['any error']}}))
