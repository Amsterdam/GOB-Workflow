import datetime
from collections import namedtuple

from unittest import TestCase, mock

from gobcore.status.heartbeat import STATUS_START, STATUS_OK, STATUS_FAIL
from gobworkflow.workflow.jobs import job_start, job_end, step_start, step_status

Job = namedtuple("Job", ["id"])
Step = namedtuple("Job", ["id"])


class TestJobManagement(TestCase):

    def setUp(self):
        pass

    @mock.patch("gobworkflow.workflow.jobs.job_save")
    def test_job_start(self, job_save):
        job_save.return_value = Job("any id")
        job = job_start("any job", {"header": {}, "a": 1, "b": "string", "c": True})
        self.assertEqual(job["name"], "any job.1.string.True")
        self.assertEqual(job["type"], "any job")
        self.assertEqual(job["args"], ["1", "string", "True"])
        self.assertIsInstance(job["start"], datetime.datetime)
        self.assertIsNone(job["end"])
        self.assertEqual(job["status"], "started")

    @mock.patch("gobworkflow.workflow.jobs.job_update", mock.MagicMock())
    def test_job_end(self):
        job = job_end("any jobid")
        self.assertEqual(job["id"], "any jobid")
        self.assertIsInstance(job["end"], datetime.datetime)
        self.assertEqual(job["status"], "ended")

    @mock.patch("gobworkflow.workflow.jobs.job_update", mock.MagicMock())
    def test_job_end_missing_id(self):
        job = job_end(None)
        self.assertIsNone(job)

    @mock.patch("gobworkflow.workflow.jobs.step_save")
    def test_step_start(self, step_save):
        step_save.return_value = Step("any id")
        step = step_start("any step", {})
        self.assertEqual(step["name"], "any step")
        self.assertIsNone(step["start"])
        self.assertIsNone(step["end"])
        self.assertEqual(step["status"], "scheduled")

    @mock.patch("gobworkflow.workflow.jobs.step_update")
    @mock.patch("gobworkflow.workflow.jobs.job_update")
    def test_step_status_start(self, mock_job_update, mock_step_update):
        step = step_status("any jobid" ,"any stepid", STATUS_START)
        mock_step_update.assert_called_with({'id': 'any stepid', 'status': 'started', 'start': mock.ANY})
        mock_job_update.assert_not_called()

    @mock.patch("gobworkflow.workflow.jobs.step_update")
    @mock.patch("gobworkflow.workflow.jobs.job_update")
    def test_step_status_ok(self, mock_job_update, mock_step_update):
        step = step_status("any jobid" ,"any stepid", STATUS_OK)
        mock_step_update.assert_called_with({'id': 'any stepid', 'status': 'ended', 'end': mock.ANY})
        mock_job_update.assert_not_called()

    @mock.patch("gobworkflow.workflow.jobs.step_update")
    @mock.patch("gobworkflow.workflow.jobs.job_update")
    def test_step_status_fail(self, mock_job_update, mock_step_update):
        step = step_status("any jobid" ,"any stepid", STATUS_FAIL)
        mock_step_update.assert_called_with({'id': 'any stepid', 'status': 'failed', 'end': mock.ANY})
        mock_job_update.assert_called_with({'id': 'any jobid', 'end': mock.ANY, 'status': 'ended'})
