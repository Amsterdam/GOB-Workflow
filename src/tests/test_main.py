import sys
import importlib

from unittest import TestCase, mock

from gobcore.status.heartbeat import STATUS_FAIL
from collections import namedtuple

class MockWorkflow:

    def handle_msg(self, msg):
        self.msg = msg

    def handle_result(self):
        return self.handle_msg

    def start(self, msg):
        self.msg = msg

class TestMain(TestCase):

    @mock.patch('gobcore.logging.logger.logger', mock.MagicMock())
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    @mock.patch('gobworkflow.storage.storage.connect')
    @mock.patch('gobworkflow.storage.storage.get_job_step')
    @mock.patch('gobworkflow.workflow.jobs.step_status')
    @mock.patch('gobworkflow.workflow.workflow.Workflow')
    def test_migrate(self, mock_workflow, mock_status, mock_get_job_step, mock_connect, mock_messagedriven_service):

        # Without command line arguments
        sys.argv = ['python -m gobworkflow', '--migrate']

        from gobworkflow import __main__
        importlib.reload(__main__)

        # Should connect to the storage
        mock_connect.assert_called_with(migrate=True)

    @mock.patch('gobcore.logging.logger.logger', mock.MagicMock())
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    @mock.patch('gobworkflow.storage.storage.connect')
    @mock.patch('gobworkflow.storage.storage.get_job_step')
    @mock.patch('gobworkflow.workflow.jobs.step_status')
    @mock.patch('gobworkflow.workflow.workflow.Workflow')
    @mock.patch('gobworkflow.workflow.hooks.handle_result')
    @mock.patch('gobworkflow.storage.storage.wait_for_storage')
    def test_main(self, mock_wait, mock_handle, mock_workflow, mock_status, mock_get_job_step, mock_connect, mock_messagedriven_service):

        # With command line arguments
        sys.argv = ['python -m gobworkflow']

        from gobworkflow import __main__
        importlib.reload(__main__)

        # Should connect to the storage
        mock_connect.assert_called_with()
        # Should check for any pending migrations
        mock_wait.assert_called_with()
        # Should start as a service
        mock_messagedriven_service.assert_called_with(__main__.SERVICEDEFINITION,
                                                 "Workflow",
                                                      {'prefetch_count': 1, 'load_message': False})

        mock_get_job_step.return_value = namedtuple('Job', ['type'])('any jobtype'),\
                                         namedtuple('Step', ['name'])('any stepname')

        workflow = MockWorkflow()
        mock_workflow.return_value = workflow
        __main__.handle_result({
            'header': {
                'jobid': 'any jobid',
                'stepid': 'any stepid'
            }
        })
        self.assertEqual(workflow.msg, {'header': {'jobid': 'any jobid', 'stepid': 'any stepid'}})

        workflow.msg = None
        __main__.handle_result({
            'header': {
                'jobid': 'any jobid',
                'stepid': 'any stepid',
                'result_key': 'any result key'
            }
        })
        self.assertIsNone(workflow.msg)
        mock_handle.assert_called()

        __main__.start_workflow({
            'workflow': {
                'workflow_name': 'any workflow',
                'step_name': 'any step'
            },
            'header': {
                'jobid': 'any job',
                'stepid': 'any step'
            },
            'anything': 'any value'
        })
        self.assertEqual(workflow.msg, {'anything': 'any value', 'header': { 'jobid': 'any job', 'stepid': 'any step' }})
        mock_workflow.assert_called_with('any workflow', 'any step')

        __main__.start_workflow({
            'workflow': {
                'workflow_name': 'any workflow',
            },
            'header': {
                'jobid': 'any job',
                'stepid': 'any step'
            },
            'anything': 'any value'
        })
        self.assertEqual(workflow.msg, {'anything': 'any value', 'header': { 'jobid': 'any job', 'stepid': 'any step' }})
        mock_workflow.assert_called_with('any workflow')

        workflow.msg = None
        __main__.start_workflow({
            'workflow': {
                'workflow_name': None,
            },
            'header': {
                'jobid': 'any job',
                'stepid': 'any step'
            },
            'anything': 'any value'
        })
        self.assertIsNone(workflow.msg)
        mock_workflow.end_of_workflow.assert_called()

        __main__.on_workflow_progress({"jobid": "any job", "stepid": "any step", "status": "any status"})
        mock_status.assert_called_with("any job", "any step", "any status")

        __main__.on_workflow_progress({"jobid": "any job", "stepid": "any step", "status": STATUS_FAIL, "info_msg": "Severe error"})
        mock_status.assert_called_with("any job", "any step", STATUS_FAIL)