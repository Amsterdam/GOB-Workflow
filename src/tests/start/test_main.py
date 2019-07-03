from unittest import TestCase, mock

from gobworkflow.workflow.config import IMPORT, EXPORT, EXPORT_TEST, RELATE, IMPORT_PREPARE

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


@mock.patch("gobworkflow.start.__main__.connect", mock.MagicMock())
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

    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_import(self, mock_argparse, mock_workflow):
        mock_start_workflow = mock.MagicMock()
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'import'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collection'] = 'collection'
        MockArgumentParser.arguments['application'] = 'application'

        import_args = {
            "catalogue": "catalogue",
            "collection": "collection",
            "application": "application"
        }
        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(IMPORT)

        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with(import_args)

    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_export(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'export'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collection'] = 'collection'
        MockArgumentParser.arguments['destination'] = 'destination'

        export_args = {
            "catalogue": "catalogue",
            "collection": "collection",
            "destination": "destination"
        }

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(EXPORT)

        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with(export_args)

    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_relate(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'relate'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collections'] = []

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(RELATE)

        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with({'catalogue': 'catalogue', 'collections': None})


    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_relate_single(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'relate'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collections'] = ['collection']

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(RELATE)

        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with({'catalogue': 'catalogue', 'collections': 'collection'})


    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_relate_multiple(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'relate'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'
        MockArgumentParser.arguments['collections'] = ['collection1', 'collection2']

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(RELATE)

        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with({'catalogue': 'catalogue', 'collections': 'collection1 collection2'})


    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_prepare(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'prepare'
        MockArgumentParser.arguments['catalogue'] = 'catalogue'

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(IMPORT, IMPORT_PREPARE)
        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with({'catalogue': 'catalogue'})


    @mock.patch('gobworkflow.start.__main__.Workflow')
    @mock.patch('argparse.ArgumentParser')
    def test_WorkflowCommands_export_test(self, mock_argparse, mock_workflow):
        mock_argparse.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'export_test'
        MockArgumentParser.arguments['catalogue'] = 'any catalogue'

        __main__.WorkflowCommands()

        mock_workflow.assert_called_with(EXPORT, EXPORT_TEST)
        instance = mock_workflow.return_value
        assert instance.start_new.call_count == 1
        instance.start_new.assert_called_with({'catalogue': 'any catalogue'})
