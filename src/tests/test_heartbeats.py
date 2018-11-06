from unittest import TestCase, mock

import datetime

from gobworkflow.heartbeats import on_heartbeat, check_services

class TestHeartbeats(TestCase):

    @mock.patch('gobworkflow.heartbeats.update_service')
    def test_on_heartbeat(self, update_service):
        service = {
            "name": "AnyService",
            "is_alive": True,
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
    def test_check_services(self, update_service):
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

        on_heartbeat(msg)

        # Assure that the service has been marked dead because of a heartbeat timeout
        self.assertEqual(update_service.call_count, 2)
        service["is_alive"] = False
        update_service.assert_called_with(service, [])
