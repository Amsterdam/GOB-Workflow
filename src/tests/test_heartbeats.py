from unittest import TestCase, mock
from collections import namedtuple

import datetime

from gobworkflow.heartbeats import on_heartbeat, check_services

class TestHeartbeats(TestCase):

    @mock.patch('gobworkflow.heartbeats.get_services')
    @mock.patch('gobworkflow.heartbeats.update_service')
    def test_on_heartbeat(self, update_service, get_services):
        service = {
            "name": "AnyService",
            "is_alive": True,
            "host": None,
            "pid": None,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        msg = {
            "threads": [
                {
                    "name": "thread1",
                    "is_alive": True
                },
                {
                    "name": "thread2",
                    "is_alive": False
                }
            ]
        }
        msg.update(service)

        on_heartbeat(msg)

        self.assertEqual(update_service.call_count, 1)

        service_parameter, tasks = update_service.call_args[0]
        self.assertEqual(service_parameter, service)
        self.assertEqual(len(tasks), len(msg["threads"]))

    @mock.patch('gobworkflow.heartbeats.update_service')
    @mock.patch('gobworkflow.heartbeats.get_services')
    @mock.patch('gobworkflow.heartbeats.remove_service')
    @mock.patch('gobworkflow.heartbeats.mark_service_dead')
    def test_check_services(self, mark_service_dead, remove_service, get_services, update_service):
        service = {
            "name": "AnyService",
            "is_alive": True,
            "timestamp": (datetime.datetime.now() - datetime.timedelta(hours=1)).isoformat(),
        }
        msg = {
            "threads": [
                {
                    "name": "thread1",
                    "is_alive": True
                },
                {
                    "name": "thread2",
                    "is_alive": False
                }
            ]
        }
        msg.update(service)

        get_services.return_value = []

        on_heartbeat(msg)

        # Assure that the service has been marked dead because of a heartbeat timeout
        self.assertEqual(get_services.call_count, 1)
        self.assertEqual(remove_service.call_count, 0)
        self.assertEqual(mark_service_dead.call_count, 0)

        Service = namedtuple('Service', ['timestamp'])
        service = Service(datetime.datetime.utcnow())
        get_services.return_value = [service]
        get_services.reset_mock()

        on_heartbeat(msg)

        # Assure that the service has been marked dead because of a heartbeat timeout
        self.assertEqual(get_services.call_count, 1)
        self.assertEqual(remove_service.call_count, 0)
        self.assertEqual(mark_service_dead.call_count, 0)

        service = Service(datetime.datetime.utcnow() - datetime.timedelta(minutes=15))
        get_services.return_value = [service]
        get_services.reset_mock()

        on_heartbeat(msg)

        # Assure that the service has been marked dead because of a heartbeat timeout
        self.assertEqual(get_services.call_count, 1)
        self.assertEqual(remove_service.call_count, 0)
        self.assertEqual(mark_service_dead.call_count, 1)

        service = Service(datetime.datetime.utcnow() - datetime.timedelta(days=1))
        get_services.return_value = [service]
        get_services.reset_mock()
        mark_service_dead.reset_mock()

        on_heartbeat(msg)

        # Assure that the service has been marked dead because of a heartbeat timeout
        self.assertEqual(get_services.call_count, 1)
        self.assertEqual(remove_service.call_count, 1)
        self.assertEqual(mark_service_dead.call_count, 0)
