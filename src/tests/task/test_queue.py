from unittest import TestCase
from unittest.mock import patch, MagicMock, call
from freezegun import freeze_time
from datetime import datetime

from gobworkflow.task.queue import TaskQueue
from gobcore.model.sa.management import Job, JobStep, Task
from gobcore.exceptions import GOBException

from gobcore.message_broker.config import WORKFLOW_EXCHANGE


class TestTaskQueue(TestCase):
    stepid = 2490
    jobid = 240
    process_id = 23

    def setUp(self) -> None:
        self.tasks = [
            {'task_name': 'task id 1', 'dependencies': []},
            {'task_name': 'task id 2', 'dependencies': ['task id 1']},
            {'task_name': 'task id 3', 'dependencies': ['task id 1', 'task id 2']},
        ]
        self.start_message = {
            'header': {
                'stepid': self.stepid,
                'jobid': self.jobid,
                'process_id': self.process_id,
                'extra': {
                    'extraheader': 'value',
                }
            },
            'contents': {
                'tasks': self.tasks,
                'key_prefix': 'pref',
                'extra_msg': {
                    'key': 'value'
                }
            }
        }

        self.result_message = {
            'header': {
                'taskid': 283
            },
            'summary': {
                'warnings': [],
                'errors': []
            }
        }

        self.task_queue = TaskQueue()

    @patch("gobworkflow.task.queue.load_message")
    @patch("gobworkflow.task.queue.get_job_step")
    @patch("gobworkflow.task.queue.json")
    def test_on_start_tasks(self, mock_json, mock_get_job_step, mock_load_message):
        self.task_queue._validate_dependencies = MagicMock()
        self.task_queue._create_tasks = MagicMock()
        self.task_queue._queue_free_tasks_for_jobstep = MagicMock()
        mock_get_job_step.return_value = Job(id=self.jobid), JobStep(id=self.stepid)
        mock_load_message.return_value = self.start_message, None

        self.task_queue.on_start_tasks(self.start_message)
        mock_load_message.assert_called_with(self.start_message, mock_json.loads, {'stream_contents': False})
        mock_get_job_step.assert_called_with(self.jobid, self.stepid)

        self.task_queue._validate_dependencies.assert_called_with(self.tasks)
        self.task_queue._create_tasks.assert_called_with(self.jobid, self.stepid, self.process_id, self.tasks,
                                                         'pref', self.start_message['contents']['extra_msg'],
                                                         self.start_message['header']['extra'])
        self.task_queue._queue_free_tasks_for_jobstep.assert_called_with(self.stepid)

    @patch("gobworkflow.task.queue.load_message")
    @patch("gobworkflow.task.queue.get_job_step")
    @patch("gobworkflow.task.queue.json")
    def test_on_start_tasks_no_step(self, mock_json, mock_get_job_step, mock_load_message):
        mock_get_job_step.return_value = Job(id=240), None
        mock_load_message.return_value = self.start_message, None

        with self.assertRaises(GOBException):
            self.task_queue.on_start_tasks(self.start_message)

    def test_validate_dependencies(self):
        self.task_queue._validate_dependencies(self.tasks)

    def test_validate_dependencies_double_id(self):
        self.tasks[2]['task_name'] = self.tasks[1]['task_name']
        with self.assertRaises(AssertionError):
            self.task_queue._validate_dependencies(self.tasks)

    def test_validate_dependencies_circular_dependency(self):
        self.tasks[0]['dependencies'] = [self.tasks[1]['task_name']]

        with self.assertRaises(GOBException):
            self.task_queue._validate_dependencies(self.tasks)

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.task_save")
    def test_create_tasks(self, mock_task_save, mock_get_tasks):
        mock_get_tasks.return_value = []
        key_prefix = "prefix",
        extra_msg = {"extra": "msg"}
        extra_header = {"extra": "header"}

        self.tasks[0]['extra_msg'] = {'extra2': 'fromtask'}

        self.task_queue._create_tasks(self.jobid, self.stepid, self.process_id, self.tasks[:2], key_prefix, extra_msg,
                                      extra_header)

        mock_task_save.assert_has_calls([
            call({
                'name': self.tasks[0]['task_name'],
                'dependencies': self.tasks[0]['dependencies'],
                'status': self.task_queue.STATUS_NEW,
                'jobid': self.jobid,
                'stepid': self.stepid,
                'key_prefix': key_prefix,
                'extra_header': extra_header,
                'extra_msg': {
                    'extra': 'msg',
                    'extra2': 'fromtask',
                },
                'process_id': self.process_id,
            }),
            call({
                'name': self.tasks[1]['task_name'],
                'dependencies': self.tasks[1]['dependencies'],
                'status': self.task_queue.STATUS_NEW,
                'jobid': self.jobid,
                'stepid': self.stepid,
                'key_prefix': key_prefix,
                'extra_header': extra_header,
                'extra_msg': extra_msg,
                'process_id': self.process_id,
            })
        ])

        mock_get_tasks.assert_called_with(self.stepid)

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    def test_create_tasks_existing_steps(self, mock_get_tasks):
        mock_get_tasks.return_value = [1, 2, 3]

        with self.assertRaises(AssertionError):
            self.task_queue._create_tasks(self.jobid, self.stepid, self.process_id, [], '', {}, {})

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.task_lock")
    @patch("gobworkflow.task.queue.task_unlock")
    @patch("gobworkflow.task.queue.task_get")
    def test_queue_free_tasks_for_jobstep(self, mock_task_get, mock_unlock, mock_lock, mock_get_tasks):
        self.task_queue._queue_task = MagicMock()
        mock_get_tasks.return_value = [
            Task(name='task1', status=self.task_queue.STATUS_COMPLETED, dependencies=[]),
            Task(name='task2', status=self.task_queue.STATUS_NEW, dependencies=['task3']),
            Task(name='task3', status=self.task_queue.STATUS_NEW, dependencies=['task1']),
        ]
        mock_task_get.return_value = mock_get_tasks.return_value[2]
        self.task_queue._queue_free_tasks_for_jobstep(self.stepid)
        mock_get_tasks.assert_called_with(self.stepid)

        self.task_queue._queue_task.assert_has_calls([
            call(mock_get_tasks.return_value[2])
        ])

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.task_lock")
    def test_queue_free_tasks_locked(self, mock_lock, mock_get_tasks):
        self.task_queue._queue_task = MagicMock()
        mock_get_tasks.return_value = [
            Task(name='task1', status=self.task_queue.STATUS_COMPLETED, dependencies=[]),
            Task(name='task2', status=self.task_queue.STATUS_NEW, dependencies=['task3']),
            Task(name='task3', status=self.task_queue.STATUS_NEW, dependencies=['task1']),
        ]
        mock_lock.return_value = False
        self.task_queue._queue_free_tasks_for_jobstep(self.stepid)
        mock_get_tasks.assert_called_with(self.stepid)

        self.task_queue._queue_task.assert_not_called()

    @patch("gobworkflow.task.queue.publish")
    @patch("gobworkflow.task.queue.task_update")
    def test_queue_task(self, mock_update, mock_publish):
        task = Task(id=123, name='task name', jobid=self.jobid, stepid=self.stepid, extra_msg={'extra': 'msg'},
                    key_prefix='prefix', process_id=self.process_id, extra_header={'extra': 'header'})

        with freeze_time():
            self.task_queue._queue_task(task)
            now = datetime.now()

        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, task.key_prefix + ".task.request", {
            'extra': 'msg',
            'taskid': task.id,
            'header': {
                'jobid': task.jobid,
                'stepid': task.stepid,
                'task_name': task.name,
                'process_id': task.process_id,
                'extra': 'header',
            }
        })

        mock_update.assert_called_with({
            'id': task.id,
            'start': now,
            'status': self.task_queue.STATUS_QUEUED,
        })

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    def test_all_tasks_complete(self, mock_get_tasks):
        mock_get_tasks.return_value = [
            Task(name='task1', status=self.task_queue.STATUS_COMPLETED),
            Task(name='task2', status=self.task_queue.STATUS_COMPLETED),
        ]
        self.assertTrue(self.task_queue._all_tasks_complete(self.stepid))

        mock_get_tasks.return_value = [
            Task(name='task1', status=self.task_queue.STATUS_COMPLETED),
            Task(name='task2', status=self.task_queue.STATUS_NEW),
        ]
        self.assertFalse(self.task_queue._all_tasks_complete(self.stepid))

        mock_get_tasks.return_value = []
        self.assertTrue(self.task_queue._all_tasks_complete(self.stepid))

    @patch("gobworkflow.task.queue.task_get")
    @patch("gobworkflow.task.queue.task_update")
    def test_on_task_result(self, mock_task_update, mock_task_get):
        self.task_queue._queue_free_tasks_for_jobstep = MagicMock()
        self.task_queue._all_tasks_complete = MagicMock(return_value=False)
        mock_task_get.return_value = Task(id=382, stepid=self.stepid)

        with freeze_time():
            self.task_queue.on_task_result(self.result_message)
            now = datetime.now()

        mock_task_update.assert_called_with({
            'id': 382,
            'status': self.task_queue.STATUS_COMPLETED,
            'summary': self.result_message['summary'],
            'end': now
        })

        self.task_queue._queue_free_tasks_for_jobstep.assert_called_with(self.stepid)

    @patch("gobworkflow.task.queue.task_get")
    @patch("gobworkflow.task.queue.task_update")
    def test_on_task_result_failed(self, mock_task_update, mock_task_get):
        self.task_queue._abort_tasks = MagicMock()
        mock_task_get.return_value = Task(id=382, stepid=self.stepid)
        self.result_message['summary']['errors'] = ['error']

        with freeze_time():
            self.task_queue.on_task_result(self.result_message)
            now = datetime.now()

        mock_task_update.assert_called_with({
            'id': 382,
            'status': self.task_queue.STATUS_FAILED,
            'summary': self.result_message['summary'],
            'end': now
        })

        self.task_queue._abort_tasks.assert_called_with(self.stepid)

    @patch("gobworkflow.task.queue.task_get")
    @patch("gobworkflow.task.queue.task_update")
    def test_on_task_result_complete(self, mock_task_update, mock_task_get):
        self.task_queue._queue_free_tasks_for_jobstep = MagicMock()
        self.task_queue._all_tasks_complete = MagicMock(return_value=True)
        self.task_queue._publish_complete = MagicMock()
        mock_task_get.return_value = Task(id=382, stepid=self.stepid)

        with freeze_time():
            self.task_queue.on_task_result(self.result_message)
            now = datetime.now()

        mock_task_update.assert_called_with({
            'id': 382,
            'status': self.task_queue.STATUS_COMPLETED,
            'summary': self.result_message['summary'],
            'end': now
        })

        self.task_queue._queue_free_tasks_for_jobstep.assert_called_with(self.stepid)
        self.task_queue._publish_complete.assert_called_with(mock_task_get.return_value)

    @patch("gobworkflow.task.queue.task_update")
    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.task_lock")
    @patch("gobworkflow.task.queue.task_unlock")
    def test_abort_tasks(self, mock_unlock, mock_lock, mock_get_tasks, mock_update):
        self.task_queue._publish_complete = MagicMock()
        mock_lock.return_value = True
        mock_get_tasks.return_value = [
            Task(id=1, name='task1', status=self.task_queue.STATUS_COMPLETED),
            Task(id=2, name='task2', status=self.task_queue.STATUS_NEW),
        ]
        self.task_queue._abort_tasks(self.stepid)
        mock_get_tasks.assert_called_with(self.stepid)
        mock_lock.assert_called_with(mock_get_tasks.return_value[1])
        mock_unlock.assert_called_with(mock_get_tasks.return_value[1])
        mock_update.assert_called_with({
            'id': 2,
            'status': self.task_queue.STATUS_ABORTED,
        })

        self.task_queue._publish_complete.assert_called_with(mock_get_tasks.return_value[0])

    @patch("gobworkflow.task.queue.task_update")
    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.task_lock")
    @patch("gobworkflow.task.queue.task_unlock")
    def test_abort_tasks_locked(self, mock_unlock, mock_lock, mock_get_tasks, mock_update):
        self.task_queue._publish_complete = MagicMock()
        mock_lock.return_value = False
        mock_get_tasks.return_value = [
            Task(id=1, name='task1', status=self.task_queue.STATUS_COMPLETED),
            Task(id=2, name='task2', status=self.task_queue.STATUS_NEW),
        ]

        self.task_queue._abort_tasks(self.stepid)
        mock_lock.assert_called_with(mock_get_tasks.return_value[1])
        mock_unlock.assert_not_called()
        self.task_queue._publish_complete.assert_called_with(mock_get_tasks.return_value[0])

    @patch("gobworkflow.task.queue.get_tasks_for_stepid")
    @patch("gobworkflow.task.queue.publish")
    def test_publish_complete(self, mock_publish, mock_get_tasks):
        summary1 = {
            'warnings': ['w1', 'w2'],
            'errors': [],
        }
        summary2 = {
            'warnings': ['w3'],
            'errors': ['e1'],
        }
        mock_get_tasks.return_value = [
            Task(id=1, name='task1', status=self.task_queue.STATUS_COMPLETED, summary=summary1),
            Task(id=2, name='task2', status=self.task_queue.STATUS_NEW, summary=summary2),
            Task(id=3, name='task3', status=self.task_queue.STATUS_ABORTED),
        ]

        task_arg = Task(stepid=self.stepid, jobid=self.jobid, key_prefix="prefix",
                        extra_msg={'extra': 'msg'}, extra_header={'extra': 'header'})

        self.task_queue._publish_complete(task_arg)
        mock_get_tasks.assert_called_with(task_arg.stepid)

        mock_publish.assert_called_with(WORKFLOW_EXCHANGE, task_arg.key_prefix + ".task.complete", {
            'extra': 'msg',
            'header': {
                'jobid': self.jobid,
                'stepid': self.stepid,
                'extra': 'header',
            },
            'summary': {
                'warnings': ['w1', 'w2', 'w3'],
                'errors': ['e1']
            }
        })
