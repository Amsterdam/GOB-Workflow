from unittest import TestCase, mock

import datetime
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError

from gobcore.model.sa.management import Job, JobStep, Task

import gobworkflow.storage

from gobworkflow.storage.storage import connect, migrate_storage, disconnect, is_connected
from gobworkflow.storage.storage import save_log, get_services, remove_service, mark_service_dead, update_service, \
    _update_servicetasks, save_audit_log
from gobworkflow.storage.storage import job_save, job_update, step_save, step_update, get_job_step, job_runs, job_get
from gobworkflow.storage.storage import task_get, task_save, task_update, task_lock, task_unlock, get_tasks_for_stepid

class MockedService:

    service_id = None
    id = None
    host = None
    name = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class MockedSession:

    def __init__(self):
        self._first = None
        self._add = None
        self._delete = None
        self._all = []
        self.filter_kwargs = {}
        self.update_args = ()
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def execute(self):
        pass

    def query(self, anyClass):
        return self

    def get(self, arg):
        return arg

    def filter_by(self, *args, **kwargs):
        self.filter_kwargs = kwargs
        return self

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self._all

    def first(self):
        return self._first

    def add(self, anyObject):
        self._add = anyObject
        return self

    def order_by(self, *args, **kwargs):
        return self

    def delete(self, anyObject=None):
        self._delete = anyObject
        return self

    def commit(self):
        pass

    def update(self, *args, **kwargs):
        self.update_args = args
        return 1

class MockedEngine:

    def dispose(self):
        pass

    def execute(self, stmt):
        self.stmt = stmt

    def begin(self):
        return self

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

class MockException(Exception):
    pass

def raise_exception(e):
    raise e("Raised")

class TestStorage(TestCase):

    def setUp(self):
        gobworkflow.storage.storage.engine = MockedEngine()
        gobworkflow.storage.storage.session = MockedSession()

    def test_update_service(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        gobworkflow.storage.storage.Service = MockedService
        gobworkflow.storage.storage.ServiceTask = MockedService

        service = {
            "name": "AnyService",
            "host": "AnyHost",
            "pid": 123,
            "is_alive": True,
            "timestamp": "timestamp"
        }

        # If the service is not found, it should be added
        mockedSession._first = None
        update_service(service, [])
        self.assertEqual(mockedSession._add.name, "AnyService")

        # If the service is found, it should be updated
        mockedSession._first = MockedService(**{"name": "AnyService", "is_alive": None, "timestamp": None})
        update_service(service, [])
        self.assertEqual(mockedSession._first.is_alive, service["is_alive"])

    @mock.patch("gobworkflow.storage.storage.migrate_storage")
    @mock.patch("gobworkflow.storage.storage.create_engine")
    def test_connect(self, mock_create, mock_migrate):
        result = connect()

        mock_create.assert_called()
        mock_migrate.assert_called()
        self.assertEqual(result, True)
        self.assertEqual(is_connected(), True)

    @mock.patch("gobworkflow.storage.storage.DBAPIError", MockException)
    @mock.patch("gobworkflow.storage.storage.create_engine", mock.MagicMock())
    @mock.patch("gobworkflow.storage.storage.migrate_storage", lambda argv: raise_exception(MockException))
    def test_connect_error(self):
        # Operation errors should be catched
        result = connect()

        self.assertEqual(result, False)
        self.assertEqual(is_connected(), False)

    @mock.patch("gobworkflow.storage.storage.migrate_storage", lambda force_migrate: raise_exception(MockException))
    @mock.patch("gobworkflow.storage.storage.create_engine", mock.MagicMock())
    def test_connect_other_error(self):
        # Only operational errors should be catched
        with self.assertRaises(MockException):
            connect()

    @mock.patch("gobworkflow.storage.storage.engine.dispose")
    @mock.patch("gobworkflow.storage.storage.session.close")
    @mock.patch("gobworkflow.storage.storage.session.rollback")
    def test_disconnect(self, mock_rollback, mock_close, mock_dispose):

        disconnect()

        mock_rollback.assert_called()
        mock_close.assert_called()
        mock_dispose.assert_called()

        self.assertEqual(gobworkflow.storage.storage.session, None)
        self.assertEqual(gobworkflow.storage.storage.engine, None)
        self.assertEqual(is_connected(), False)

    @mock.patch("gobworkflow.storage.storage.DBAPIError", MockException)
    @mock.patch("gobworkflow.storage.storage.engine.dispose", lambda: raise_exception(MockException))
    @mock.patch("gobworkflow.storage.storage.session.close", mock.MagicMock())
    @mock.patch("gobworkflow.storage.storage.session.rollback", mock.MagicMock())
    def test_disconnect_operational_error(self):
        # Operation errors should be catched

        disconnect()

        self.assertEqual(gobworkflow.storage.storage.session, None)
        self.assertEqual(gobworkflow.storage.storage.engine, None)

    @mock.patch("gobworkflow.storage.storage.engine.dispose", lambda: raise_exception(MockException))
    @mock.patch("gobworkflow.storage.storage.session.close", mock.MagicMock())
    @mock.patch("gobworkflow.storage.storage.session.rollback", mock.MagicMock())
    def test_disconnect_other_error(self):
        # Only operational errors should be catched

        with self.assertRaises(MockException):
            disconnect()

    def test_is_connected_not_ok(self):
        result = is_connected()
        self.assertEqual(result, False)

    @mock.patch("gobworkflow.storage.storage.session.execute", mock.MagicMock())
    def test_is_connected_ok(self):
        result = is_connected()
        self.assertEqual(result, True)

    @mock.patch("gobworkflow.storage.storage.session.add")
    @mock.patch("gobworkflow.storage.storage.session.commit")
    def test_save_log(self, mock_commit, mock_add):
        msg = {
            "timestamp": "2020-06-20T12:20:20.000"
        }

        save_log(msg)

        mock_add.assert_called_with(mock.ANY)
        mock_commit.assert_called_with()

    @mock.patch("gobworkflow.storage.storage.IntegrityError", MockException)
    @mock.patch("gobworkflow.storage.storage.session.add")
    @mock.patch("gobworkflow.storage.storage.session.commit")
    @mock.patch("gobworkflow.storage.storage.session.rollback")
    def test_save_log_with_exception(self, mock_rollback, mock_commit, mock_add):
        msg = {
            "timestamp": "2020-06-20T12:20:20.000"
        }
        mock_add.side_effect = lambda r: raise_exception(MockException)
        save_log(msg)
        mock_add.assert_called_with(mock.ANY)
        mock_commit.assert_not_called()
        mock_rollback.assert_called()

    @mock.patch("gobworkflow.storage.storage.session")
    @mock.patch("gobworkflow.storage.storage.datetime.datetime")
    @mock.patch("gobworkflow.storage.storage.AuditLog")
    def test_save_audit_log(self, mock_audit_log, mock_datetime, mock_session):
        msg = {
            'timestamp': 'the timestamp',
            'source': 'the source',
            'destination': 'the destination',
            'type': 'the type',
            'data': 'the data',
            'request_uuid': 'the uuid',
        }

        save_audit_log(msg)

        mock_audit_log.assert_called_with(
            timestamp=mock_datetime.strptime.return_value,
            source='the source',
            destination='the destination',
            type='the type',
            data='the data',
            request_uuid='the uuid',
        )

        mock_session.add.assert_called_with(mock_audit_log.return_value)
        mock_session.commit_assert_called_once()


    def test_update_servicetasks(self):
        gobworkflow.storage.storage.Service = MockedService
        gobworkflow.storage.storage.ServiceTask = MockedService

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # No action on empty lists
        _update_servicetasks(MockedService(), tasks=[])
        self.assertEqual(mockedSession._add, None)
        self.assertEqual(mockedSession._delete, None)

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # add task when not yet exists
        _update_servicetasks(MockedService(), tasks=[{"name": "AnyTask"}])
        self.assertEqual(mockedSession._add.name, "AnyTask")

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # delete task when it no longer exists
        _update_servicetasks(MockedService(), tasks=[])
        self.assertEqual(mockedSession._delete, None)

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        mocked_task = MockedService(**{"name": "AnyTask", "is_alive": None})
        other_task = MockedService(**{"name": "AnyTask2", "is_alive": None})
        # update task
        mockedSession._all = [mocked_task, other_task]
        _update_servicetasks(MockedService(), tasks=[{"name": "AnyTask", "is_alive": True}])
        self.assertEqual(mocked_task.is_alive, True)

    def test_get_services(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        services = get_services()
        self.assertEqual(services, [])

    @mock.patch("gobworkflow.storage.storage._update_servicetasks")
    def test_mark_as_dead(self, mock_update_servicetasks):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession

        mockedService = MockedService()
        mark_service_dead(mockedService)

        self.assertEqual(mockedService.is_alive, False)
        mock_update_servicetasks.assert_called_with(mockedService, [])

    @mock.patch("gobworkflow.storage.storage.ObjectDeletedError", MockException)
    @mock.patch("gobworkflow.storage.storage._mark_dangling_tasks")
    @mock.patch("gobworkflow.storage.storage.session")
    def test_update_servicetasks_failure(self, mock_session, mock_mark_dangling_tasks):
        mocked_service = MockedService()

        mock_mark_dangling_tasks.side_effect = lambda c, s, l: raise_exception(MockException)
        result = _update_servicetasks(mocked_service, [])
        self.assertIsNone(result)
        mock_session.query.assert_called_with(MockedService)

    def test_remove_service(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession

        mockedService = MockedService()
        remove_service(mockedService)

        self.assertEqual(mockedSession._delete, None)

    def test_job_save(self):
        result = job_save({"name": "any name"})
        self.assertIsInstance(result, Job)
        self.assertEqual(result.name, "any name")

    def test_job_update(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        mockedSession.get = lambda id: Job()

        result = job_update({"id": 123})
        self.assertIsInstance(result, Job)
        self.assertEqual(result.id, 123)

    def test_job_get(self):
        result = job_get('someid')
        self.assertEqual('someid', result)

    def test_step_save(self):
        result = step_save({"name": "any name"})
        self.assertIsInstance(result, JobStep)
        self.assertEqual(result.name, "any name")

    def test_step_update(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        mockedSession.get = lambda id: JobStep()

        result = step_update({"id": 123})
        self.assertIsInstance(result, JobStep)
        self.assertEqual(result.id, 123)

    def test_get_job_step(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        job, step = get_job_step(1, 2)
        self.assertEqual(job, 1)
        self.assertEqual(step, 2)

    def test_task_get(self):
        result = task_get('someid')
        self.assertEqual('someid', result)

    def test_task_save(self):
        result = task_save({"name": "any name"})
        self.assertIsInstance(result, Task)
        self.assertEqual(result.name, "any name")

    def test_task_update(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        mockedSession.get = lambda id: Task()

        result = task_update({"id": 123})
        self.assertIsInstance(result, Task)
        self.assertEqual(result.id, 123)

    def test_task_lock(self):
        mock_session = MockedSession()
        gobworkflow.storage.storage.session = mock_session

        result = task_lock(Task())
        self.assertTrue(result)
        self.assertTrue(len(mock_session.update_args) == 1)
        self.assertTrue(type(mock_session.update_args[0]['lock']) == int)

    def test_task_lock_fail(self):
        mock_session = MockedSession()
        gobworkflow.storage.storage.session = mock_session
        mock_session.update = lambda _: 0
        result = task_lock(Task())
        self.assertFalse(result)

    def test_task_unlock(self):
        mock_session = MockedSession()
        gobworkflow.storage.storage.session = mock_session
        task_unlock(Task())
        self.assertEqual(({'lock': None},), mock_session.update_args)

    def test_get_tasks_for_stepid(self):
        mock_session = MockedSession()
        gobworkflow.storage.storage.session = mock_session
        mock_session.all = lambda: ['a', 'b']
        result = get_tasks_for_stepid("someid")
        self.assertEqual({"stepid": "someid"}, mock_session.filter_kwargs)
        self.assertEqual(['a', 'b'], result)

    @mock.patch("gobworkflow.storage.storage.alembic.config")
    @mock.patch('gobworkflow.storage.storage.alembic.script')
    @mock.patch('gobworkflow.storage.storage.migration')
    def test_migrate_storage(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 1"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        migrate_storage(force_migrate=True)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)
        mock_config.main.assert_called()

    @mock.patch("gobworkflow.storage.storage.alembic.config")
    @mock.patch('gobworkflow.storage.storage.alembic.script')
    @mock.patch('gobworkflow.storage.storage.migration')
    def test_migrate_storage_up_to_date(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 2"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        migrate_storage(force_migrate=False)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)
        mock_config.main.assert_not_called()

    @mock.patch("gobworkflow.storage.storage.alembic.config")
    @mock.patch('gobworkflow.storage.storage.alembic.script')
    @mock.patch('gobworkflow.storage.storage.migration')
    def test_migrate_storage_exception(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 1"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        mock_config.main = lambda argv: raise_exception(MockException)

        migrate_storage(force_migrate=False)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)


class TestJobRuns(TestCase):

    @mock.patch('gobworkflow.storage.storage.session')
    def test_job_runs(self, mock_session):
        session = MockedSession()
        mock_session.query.return_value = session

        job_info = {'id': 'any id', 'name': 'any name'}

        session._first = None
        result = job_runs(job_info)
        self.assertEqual(result, False)

        class Job:
            def __init__(self, start):
                self.start = start
                self.id = 'any id'

        job = Job( datetime.datetime.now())
        session._first = job
        result = job_runs(job_info)
        self.assertEqual(result, True)

        job.start = datetime.datetime.now() - datetime.timedelta(hours=11)
        result = job_runs(job_info)
        self.assertEqual(result, True)

        job.start = datetime.datetime.now() - datetime.timedelta(hours=12)
        result = job_runs(job_info)
        self.assertEqual(result, False)
