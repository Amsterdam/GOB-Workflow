from unittest import TestCase, mock

class MockedLogger:

    def __init__(self):
        pass

    def info(self, msg, extra):
        self.msg = msg
        self.extra = extra

class TestMain(TestCase):

    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    @mock.patch('gobcore.log.get_logger')
    @mock.patch('gobworkflow.storage.storage.connect')
    def test_main(self, connect, get_logger, messagedriven_service):

        from gobworkflow import __main__

        # Should connect to the storage
        connect.assert_called()
        # Should require a logger
        get_logger.assert_called_with(name="WORKFLOW")
        # Should start as a service
        messagedriven_service.assert_called_with(__main__.SERVICEDEFINITION, "Workflow")

        __main__.logger = MockedLogger()
        __main__.pass_through({
            "header": {
                "process_id": "AnyProcessId",
                "source": "AnySource",
                "entity": "AnyEntity"
            }
        }, "import")
        self.assertEqual(__main__.logger.msg, "Import proposal accepted")
        self.assertEqual(__main__.logger.extra["process_id"], "AnyProcessId")

        __main__.pass_through({
            "header": {
                "process_id": "AnyProcessId",
                "source": "AnySource",
                "entity": "AnyEntity"
            }
        }, "update")
        self.assertEqual(__main__.logger.msg, "Update proposal accepted")
