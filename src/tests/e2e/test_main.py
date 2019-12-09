import os

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobworkflow.e2e.__main__ import E2ETest, IMPORT, RELATE, run_e2e_tests


class TestE2ETest(TestCase):

    @patch("gobworkflow.e2e.__main__.Workflow")
    def test_start_import(self, mock_workflow):
        e2e = E2ETest()
        e2e._wait_job_finished = MagicMock()
        e2e._start_import('some catalog', 'some collection', 'some application')

        mock_workflow.assert_called_with(IMPORT)
        mock_workflow.return_value.start_new.assert_called_with({
            'catalogue': 'some catalog',
            'collection': 'some collection',
            'application': 'some application',
        })
        e2e._wait_job_finished.assert_called_with(mock_workflow.return_value.start_new.return_value)

    @patch("gobworkflow.e2e.__main__.Workflow")
    def test_start_relate(self, mock_workflow):
        e2e = E2ETest()
        e2e._wait_job_finished = MagicMock()
        e2e._start_relate('some catalog')

        mock_workflow.assert_called_with(RELATE)
        mock_workflow.return_value.start_new.assert_called_with({
            'catalogue': 'some catalog',
        })
        e2e._wait_job_finished.assert_called_with(mock_workflow.return_value.start_new.return_value)

    @patch("gobworkflow.e2e.__main__.job_get")
    def test_wait_job_finished(self, mock_job_get):
        e2e = E2ETest()
        e2e._exit_error = MagicMock()
        mock_job_get.return_value = type('MockJob', (object,), {'id': 2840, 'status': 'ended'})

        e2e._wait_job_finished({'id': 2840})
        e2e._exit_error.assert_not_called()

    @patch("gobworkflow.e2e.__main__.job_get")
    def test_wait_job_finished_rejected(self, mock_job_get):
        e2e = E2ETest()
        e2e._exit_error = MagicMock()
        mock_job_get.return_value = type('MockJob', (object,), {'id': 2840, 'status': 'rejected'})
        e2e._wait_job_finished({'id': 2840})

        e2e._exit_error.assert_called_with("Job 2840 rejected")

    @patch("gobworkflow.e2e.__main__.job_get")
    def test_wait_job_finished_timeout(self, mock_job_get):
        e2e = E2ETest()
        e2e._exit_error = MagicMock()
        e2e.MAX_WAIT_JOB_FINISHED = 1
        mock_job_get.return_value = type('MockJob', (object,), {'id': 2840, 'status': 'started'})
        e2e._wait_job_finished({'id': 2840})

        e2e._exit_error.assert_called_with("Job 2840 took too long to finish")

    @patch("gobworkflow.e2e.__main__.requests.get")
    def test_check_api_output(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nC"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest()
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._log_result = MagicMock()
        e2e.api_base = 'API_BASE'

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')

        e2e._log_result.assert_called_with('Test API Output', True)
        mock_get.assert_called_with('API_BASE/some/endpoint')

    @patch("gobworkflow.e2e.__main__.requests.get")
    def test_check_api_output_error_status_code(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nC"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 500, 'text': api_result})
        e2e = E2ETest()
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._exit_error = MagicMock()

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')
        e2e._exit_error.assert_called_with('Error requesting /some/endpoint')

    @patch("gobworkflow.e2e.__main__.requests.get")
    def test_check_api_output_mismatch_result(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nD"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest()
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._log_result = MagicMock()

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')
        e2e._log_result.assert_called_with('Test API Output', False)

    @patch("builtins.print")
    def test_log(self, mock_print):
        e2e = E2ETest()
        e2e._log("Some log message")
        mock_print.assert_called_with("Some log message")

    @patch("builtins.exit")
    def test_exit_error(self, mock_exit):
        e2e = E2ETest()
        e2e._log = MagicMock()
        e2e._exit_error("Some error message")

        e2e._log.assert_called_with("Some error message")
        mock_exit.assert_called_with(-1)

    def test_log_result(self):
        e2e = E2ETest()
        e2e.failure_cnt = 0
        e2e._log = MagicMock()

        e2e._log_result("The step", True)

        e2e._log.assert_called_with("The step                                 \033[32mOK\033[0m")
        self.assertEqual(0, e2e.failure_cnt)

        e2e._log_result("The step with error", False)
        e2e._log.assert_called_with("The step with error                      \033[31mFAIL\033[0m")
        self.assertEqual(1, e2e.failure_cnt)

    @patch("builtins.open")
    def test_load_testfile(self, mock_open):
        e2e = E2ETest()

        self.assertEqual(mock_open.return_value.__enter__.return_value.read.return_value,
                         e2e._load_testfile('filename'))

        self.assertTrue(mock_open.call_args[0][0].endswith(os.path.join('expect', 'filename')))

    def test_imports(self):
        e2e = E2ETest()
        e2e.test_import_sources = ['sourceA']
        e2e._start_import = MagicMock()
        e2e._check_api_output = MagicMock()

        e2e._test_imports()
        e2e._start_import.assert_called_with('test_catalogue', 'test_entity', 'sourceA')
        e2e._check_api_output.assert_called_with(
            '/test_catalogue/test_entity/?ndjson=true',
            'expect.sourceA.ndjson',
            'Import sourceA',
        )

    def test_relations(self):
        e2e = E2ETest()
        e2e.test_relation_entities = ['entityA']
        e2e._start_import = MagicMock()
        e2e._start_relate = MagicMock()
        e2e.test_relation_src_entities = ['srcEntityA']
        e2e.test_relation_dst_relations = ['dstRelationA']
        e2e._check_api_output = MagicMock()

        e2e._test_relations()
        e2e._start_import.assert_called_with('test_catalogue', 'entityA', 'REL')
        e2e._start_relate.assert_called_with('test_catalogue')
        e2e._check_api_output.assert_called_with(
            '/dump/rel/tst_srcEntityA_tst_dstRelationA/?format=csv',
            'expect.tst_srcEntityA_tst_dstRelationA.ndjson',
            'Relation tst_srcEntityA_tst_dstRelationA'
        )

    def test_run(self):
        e2e = E2ETest()
        e2e._test_imports = MagicMock()
        e2e._test_relations = MagicMock()
        e2e.failure_cnt = 1442

        self.assertEqual(1442, e2e.run())
        e2e._test_imports.assert_called_once()
        e2e._test_relations.assert_called_once()


class TestInit(TestCase):

    @patch("gobworkflow.e2e.__main__.connect")
    @patch("gobworkflow.e2e.__main__.E2ETest")
    @patch("gobworkflow.e2e.__main__.__name__", "__main__")
    @patch("builtins.exit")
    def test_run_e2e_tests(self, mock_exit, mock_e2e, mock_connect):
        run_e2e_tests()
        mock_connect.assert_called_once()
        mock_exit.assert_called_with(mock_e2e.return_value.run.return_value)
