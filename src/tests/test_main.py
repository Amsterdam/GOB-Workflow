from unittest import TestCase, mock

from collections import namedtuple

class MockWorkflow:

    def handle_msg(self, msg):
        self.msg = msg

    def handle_result(self):
        return self.handle_msg

class TestMain(TestCase):

    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    @mock.patch('gobworkflow.storage.storage.connect')
    @mock.patch('gobworkflow.storage.storage.get_job_step')
    @mock.patch('gobworkflow.workflow.workflow.Workflow')
    def test_main(self, mock_workflow, mock_get_job_step, mock_connect, mock_messagedriven_service):

        from gobworkflow import __main__

        # Should connect to the storage
        mock_connect.assert_called()
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
