from unittest import TestCase, mock

class TestMain(TestCase):

    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    @mock.patch('gobworkflow.storage.storage.connect')
    def test_main(self, connect, messagedriven_service):

        from gobworkflow import __main__

        # Should connect to the storage
        connect.assert_called()
        # Should start as a service
        messagedriven_service.assert_called_with(__main__.SERVICEDEFINITION, "Workflow")
