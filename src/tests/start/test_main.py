from unittest import TestCase, mock
from unittest.mock import MagicMock

from gobcore.workflow.start_commands import StartCommand, StartCommandArgument, NoSuchCommandException

from gobworkflow.start import __main__
from gobworkflow.start.__main__ import WorkflowCommands

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

@mock.patch("argparse.ArgumentParser")
@mock.patch("gobworkflow.start.__main__.StartCommands")
class TestWorkflowCommands(TestCase):

    def _mock_start_commands(self):
        mock = MagicMock()

        mock.get_all.return_value = {
            'command_a': StartCommand('command_a', {'description': 'Description of command A', 'workflow': 'wf a'}),
            'command_b': StartCommand('command_b', {'description': 'Description of command B', 'workflow': 'wf b'}),
        }
        return mock

    @mock.patch("gobworkflow.start.__main__.WorkflowCommands.execute_command")
    def test_init(self, mock_execute, mock_start_commands, mock_parser):
        mock_start_commands.return_value = self._mock_start_commands()
        mock_parser.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'command_a'
        WorkflowCommands()

        args, kwargs = mock_parser.call_args
        self.assertTrue(f"{'command_a':16s}Description of command A" in kwargs['usage'])
        self.assertTrue(f"{'command_b':16s}Description of command B" in kwargs['usage'])

        mock_start_commands.return_value.get.assert_called_with('command_a')
        mock_execute.assert_called_with(mock_start_commands.return_value.get.return_value)

    def test_init_invalid_command(self, mock_start_commands, mock_parser):
        mock_start_commands.return_value = self._mock_start_commands()
        mock_start_commands.return_value.get.side_effect = NoSuchCommandException
        mock_parser.return_value = MockArgumentParser()
        MockArgumentParser.arguments['command'] = 'nonexistent'

        with self.assertRaises(SystemExit) as cm:
            WorkflowCommands()

        self.assertEqual(cm.exception.code, 1)

    @mock.patch("gobworkflow.start.__main__.WorkflowCommands.execute_command")
    def test_extract_parser_arg_kwargs_minimal(self, mock_execute, mock_start_commands, mock_parser):
        wfc = WorkflowCommands()
        start_command_arg = StartCommandArgument({'name': 'command name'})
        expected_result = {
            'type': str,
            'help': '',
            'nargs': '?',
        }

        self.assertEqual(expected_result, wfc._extract_parser_arg_kwargs(start_command_arg))

    @mock.patch("gobworkflow.start.__main__.WorkflowCommands.execute_command")
    def test_extract_parser_arg_kwargs_maximal(self, mock_execute, mock_start_commands, mock_parser):
        wfc = WorkflowCommands()
        start_command_arg = StartCommandArgument({
            'name': 'command name',
            'default': 'default value',
            'choices': ['a', 'b'],
            'required': True,
        })

        expected_result = {
            'type': str,
            'help': '',
            'default': 'default value',
            'choices': ['a', 'b']
        }

        self.assertEqual(expected_result, wfc._extract_parser_arg_kwargs(start_command_arg))

    @mock.patch("gobworkflow.start.__main__.WorkflowCommands.execute_command")
    def test_parse_argument(self, mock_execute, mock_start_commands, mock_parser):
        wfc = WorkflowCommands()

        mock_parser.return_value = MockArgumentParser()
        MockArgumentParser.arguments['arg1'] = 'val1'
        MockArgumentParser.arguments['arg2'] = 'val2'
        wfc._extract_parser_arg_kwargs = MagicMock()

        start_command = StartCommand('command', {'workflow': 'theworkflow'})
        start_command.args = [
            StartCommandArgument({'name': 'arg1'}),
            StartCommandArgument({'name': 'arg2'}),
        ]

        result = wfc._parse_arguments(start_command)

        self.assertEqual({
            'arg1': 'val1',
            'arg2': 'val2',
        }, result)

    @mock.patch("gobworkflow.start.__main__.Workflow")
    def test_execute_command_without_step(self, mock_workflow, mock_start_commands, mock_parser):
        wfc = WorkflowCommands()
        wfc._parse_arguments = MagicMock()
        start_command = StartCommand('command', {'workflow': 'theworkflow'})

        wfc.execute_command(start_command)
        mock_workflow.assert_called_with('theworkflow')
        mock_workflow.return_value.start_new.assert_called_with(wfc._parse_arguments.return_value)

    @mock.patch("gobworkflow.start.__main__.Workflow")
    def test_execute_command_with_step(self, mock_workflow, mock_start_commands, mock_parser):
        wfc = WorkflowCommands()
        wfc._parse_arguments = MagicMock()
        start_command = StartCommand('command', {'workflow': 'theworkflow', 'start_step': 'thestartstep'})

        wfc.execute_command(start_command)
        mock_workflow.assert_called_with('theworkflow', 'thestartstep')
        mock_workflow.return_value.start_new.assert_called_with(wfc._parse_arguments.return_value)
