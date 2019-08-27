"""Start relate jobs

Requires one or more catalogs to build relations to, e.g.:

     python -m gobworkflow.start import meetbouten meetbouten
"""
import argparse
import sys

from gobcore.workflow.start_commands import StartCommands, StartCommand, NoSuchCommandException, StartCommandArgument
from gobworkflow.storage.storage import connect
from gobworkflow.workflow.workflow import Workflow


class WorkflowCommands():

    def __init__(self):
        start_commands = StartCommands()

        usage = '''<command> [<args>]

The GOB workflow commands are:'''

        for name, command in start_commands.get_all().items():
            usage += f'''
    {name:16s}{command.description}'''

        parser = argparse.ArgumentParser(
            prog='python -m gobworkflow.start',
            description='Start GOB Jobs',
            epilog='Generieke Ontsluiting Basisregistraties',
            usage=usage
        )
        parser.add_argument('command', help='Command to run')
        args = parser.parse_args(sys.argv[1:2])

        try:
            command = start_commands.get(args.command)
            self.execute_command(command)
        except NoSuchCommandException:
            print("Unrecognized command")
            parser.print_help()
            exit(1)

    def _extract_parser_arg_kwargs(self, arg: StartCommandArgument):
        kwargs = {
            'type': str,
            'help': arg.description,
        }

        if not arg.required:
            kwargs['nargs'] = '?'

        if arg.default:
            kwargs['default'] = arg.default

        if arg.choices:
            kwargs['choices'] = arg.choices

        return kwargs

    def _parse_arguments(self, command: StartCommand):
        """Parse and validate arguments

        :param command:
        :return:
        """
        parser = argparse.ArgumentParser(description=command.description)

        names = []
        for arg in command.args:
            kwargs = self._extract_parser_arg_kwargs(arg)

            parser.add_argument(arg.name, **kwargs)
            names.append(arg.name)

        input_args = parser.parse_args(sys.argv[2:])

        input_values = {}
        for name in names:
            if getattr(input_args, name):
                input_values[name] = getattr(input_args, name)

        return input_values

    def execute_command(self, command: StartCommand):
        """Executes input command

        :param command:
        :return:
        """
        input_args = self._parse_arguments(command)
        args = [command.workflow]

        if command.start_step:
            args.append(command.start_step)

        Workflow(*args).start_new(input_args)


def init():
    if __name__ == '__main__':
        connect()
        WorkflowCommands()


init()
