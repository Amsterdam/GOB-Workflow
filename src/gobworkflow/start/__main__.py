"""Start relate jobs

Requires one or more catalogs to build relations to, e.g.:

     python -m gobworkflow.start import meetbouten meetbouten
"""
import argparse
import json
import sys

from gobcore.workflow.start_commands import NoSuchCommandException, StartCommand, StartCommandArgument, StartCommands

from gobworkflow.storage.storage import connect
from gobworkflow.workflow.config import WORKFLOWS
from gobworkflow.workflow.workflow import Workflow


class WorkflowCommands:
    def __init__(self):
        start_commands = StartCommands()

        usage = f"""[info | <command> [--user USER] [<args>]]

    {"info":16s}Shows the workflows

The GOB workflow commands are:"""

        for name, command in start_commands.get_all().items():
            usage += f"""
    {name:16s}{command.description}"""

        parser = argparse.ArgumentParser(
            prog="python -m gobworkflow.start",
            description="Start GOB Jobs",
            epilog="Generieke Ontsluiting Basisregistraties",
            usage=usage,
        )
        parser.add_argument("command", help="Command to run")
        args = parser.parse_args(sys.argv[1:2])

        if args.command == "info":
            self.show_workflows()
        else:
            try:
                command = start_commands.get(args.command)
                self.execute_command(command)
            except NoSuchCommandException:
                print("Unrecognized command")
                parser.print_help()
                exit(1)

    def show_workflows(self):
        print(json.dumps(WORKFLOWS, indent=4, default=lambda o: ""))

    def _extract_parser_arg_kwargs(self, arg: StartCommandArgument):
        kwargs = {
            "help": arg.description,
        }

        if arg.action:
            kwargs["action"] = arg.action

        # If action 'store_true', the rest of the args should not be added
        if kwargs.get("action") == "store_true":
            return kwargs

        kwargs["type"] = str

        if not arg.required:
            kwargs["nargs"] = "?"

        if arg.default:
            kwargs["default"] = arg.default

        if arg.choices:
            kwargs["choices"] = arg.choices

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

            if arg.named:
                parser.add_argument(f"--{arg.name}", **kwargs)
            else:
                parser.add_argument(arg.name, **kwargs)
            names.append(arg.name)

        parser.add_argument("--user", help="User id that starts the command", required=False)
        names.append("user")
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
    if __name__ == "__main__":
        connect()
        WorkflowCommands()


init()
