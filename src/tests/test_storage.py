from unittest import TestCase, mock

import argparse
import getpass

import gobworkflow.storage
from gobworkflow.storage.storage import get_services, remove_service, mark_service_dead, update_service, _update_tasks

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
        pass

    def query(self, anyClass):
        return self

    def filter_by(self, *args, **kwargs):
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

    def delete(self, anyObject=None):
        self._delete = anyObject
        return self

    def commit(self):
        pass

class MockedEngine:

    def execute(self, stmt):
        self.stmt = stmt

class TestStorage(TestCase):

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

    def test_update_tasks(self):
        gobworkflow.storage.storage.Service = MockedService
        gobworkflow.storage.storage.ServiceTask = MockedService

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # No action on empty lists
        _update_tasks(MockedService(), tasks=[])
        self.assertEqual(mockedSession._add, None)
        self.assertEqual(mockedSession._delete, None)

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # add task when not yet exists
        _update_tasks(MockedService(), tasks=[{"name": "AnyTask"}])
        self.assertEqual(mockedSession._add.name, "AnyTask")

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        # delete task when it no longer exists
        _update_tasks(MockedService(), tasks=[])
        self.assertEqual(mockedSession._delete, None)

        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        mocked_task = MockedService(**{"name": "AnyTask", "is_alive": None})
        other_task = MockedService(**{"name": "AnyTask2", "is_alive": None})
        # update task
        mockedSession._all = [mocked_task, other_task]
        _update_tasks(MockedService(), tasks=[{"name": "AnyTask", "is_alive": True}])
        self.assertEqual(mocked_task.is_alive, True)

    def test_get_services(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession
        services = get_services()
        self.assertEqual(services, [])

    @mock.patch("gobworkflow.storage.storage._update_tasks")
    def test_mark_as_dead(self, mock_update_tasks):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession

        mockedService = MockedService()
        mark_service_dead(mockedService)

        self.assertEqual(mockedService.is_alive, False)
        mock_update_tasks.assert_called_with(mockedService, [])

    def test_remove_service(self):
        mockedSession = MockedSession()
        gobworkflow.storage.storage.session = mockedSession

        mockedService = MockedService()
        remove_service(mockedService)

        self.assertEqual(mockedSession._delete, None)
