import argparse
from unittest import TestCase, mock

from gobcore.message_broker.config import EXPORT_QUEUE, IMPORT_QUEUE, REQUEST_QUEUE

from gobworkflow.start import __main__

class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class MockArgumentParser:

    def __init__(self):
        MockArgumentParser.arguments = {}

    def add_argument(self, command, **kwargs):
        pass

    def parse_args(self, *args):
        return Struct(**MockArgumentParser.arguments)

    def print_help(self):
        pass


class TestStart(TestCase):


    def test_init(self):
        with mock.patch.object(__main__, "WorkflowCommands", return_value=42):
            with mock.patch.object(__main__, "__name__", "__main__"):
                __main__.init()

    @mock.patch('gobworkflow.start.__main__.WorkflowCommands.import_command')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_valid_command(self, mock_argparse, mock_import):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'import'
        __main__.WorkflowCommands()

        mock_import.asset_called()

    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_invalid_command(self, mock_argparse):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'fake'
        with self.assertRaises(SystemExit) as cm:
            __main__.WorkflowCommands()

        self.assertEqual(cm.exception.code, 1)

    @mock.patch('gobcore.message_broker.publish')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_import(self, mock_argparse, mock_publish):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'import'
        MockArgumentParser.arguments['dataset_file'] = 'dataset_file'

        __main__.WorkflowCommands()

        mock_publish.asset_called_with(IMPORT_QUEUE, "import.start", {"dataset_file": "dataset_file"})

    @mock.patch('gobcore.message_broker.publish')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_export(self, mock_argparse, mock_publish):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'export'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collection'] = 'collection'
        MockArgumentParser.arguments['filename'] = 'filename'
        MockArgumentParser.arguments['destination'] = 'destination'

        export_args = {
            "catalogue": "catalogue",
            "collection": "collection",
            "filename": "filename",
            "destination": "destination"
        }

        __main__.WorkflowCommands()

        mock_publish.asset_called_with(EXPORT_QUEUE, "export.start", export_args)

    @mock.patch('gobcore.message_broker.publish')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_relate(self, mock_argparse, mock_publish):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'relate'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'

        __main__.WorkflowCommands()

        mock_publish.asset_called_with(REQUEST_QUEUE, "fullrelate.request", {"catalogue": "catalogue"})
