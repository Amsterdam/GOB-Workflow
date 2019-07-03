"""Start relate jobs

Requires one or more catalogs to build relations to:

     python -m gobworkflow.start meetbouten
     python -m gobworkflow.start meetbouten nap
"""
import argparse
import sys

from gobworkflow.storage.storage import connect
from gobworkflow.workflow.workflow import Workflow
from gobworkflow.workflow.config import IMPORT, EXPORT, EXPORT_TEST, RELATE, IMPORT_PREPARE


class WorkflowCommands():

    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='python -m gobworkflow.start',
            description='Start GOB Jobs',
            epilog='Generieke Ontsluiting Basisregistraties',
            usage='''<command> [<args>]

The GOB workflow commands are:
   import       Start an import job for a collection
   export       Start an export job for a collection
   export_test  Start tests of exports for a collection
   relate       Build relations for a catalog
   prepare      Prepare a dataset for import
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        command = f"{args.command}_command"
        if not hasattr(self, command):
            print("Unrecognized command")
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, command)()

    def prepare_command(self):
        parser = argparse.ArgumentParser(description='Start a prepare job for a collection')
        parser.add_argument('catalogue',
                            type=str,
                            help='the name of the data catalog (example: "brk"')
        # Skip the first argument
        args = parser.parse_args(sys.argv[2:])
        start_args = {
            'catalogue': args.catalogue,
        }
        Workflow(IMPORT, IMPORT_PREPARE).start_new(start_args)

    def import_command(self):
        parser = argparse.ArgumentParser(description='Start an import job for a collection')
        parser.add_argument('catalogue',
                            type=str,
                            help='the name of the data catalog (example: "meetbouten"')
        parser.add_argument('collection',
                            type=str,
                            help='the name of the data collection (example: "metingen"')
        parser.add_argument('application',
                            type=str,
                            nargs='?',
                            help='the name of the application to import from')

        # Skip the first argument
        args = parser.parse_args(sys.argv[2:])
        start_args = {
            "catalogue": args.catalogue,
            "collection": args.collection,
        }

        if args.application:
            start_args['application'] = args.application

        Workflow(IMPORT).start_new(start_args)

    def export_command(self):
        parser = argparse.ArgumentParser(description='Start an export job for a collection')
        parser.add_argument('catalogue',
                            type=str,
                            help='the name of the data catalog (example: "meetbouten"')
        parser.add_argument('collection',
                            type=str,
                            help='the name of the data collection (example: "metingen"')
        parser.add_argument('destination',
                            nargs='?',
                            type=str,
                            default="Objectstore",
                            choices=["Objectstore", "File"],
                            help='destination, default is Objectstore')
        # Skip the first argument
        args = parser.parse_args(sys.argv[2:])
        export_args = {
            "catalogue": args.catalogue,
            "collection": args.collection,
            "destination": args.destination
        }
        print(f"Trigger export of {args.catalogue}.{args.collection} on {args.destination}")
        Workflow(EXPORT).start_new(export_args)

    def relate_command(self):
        parser = argparse.ArgumentParser(description='Build relations for a catalog')
        parser.add_argument('catalogue',
                            type=str,
                            help='the name of the data catalog (example: "meetbouten"')
        parser.add_argument('collections',
                            nargs='*',
                            type=str,
                            default='',
                            help='a space separated list of collections (example: "meetbouten metingen"')
        # Skip the first argument
        args = parser.parse_args(sys.argv[2:])
        collections = ' '.join(args.collections)
        print(f"Trigger build relations for {args.catalogue} {collections}")
        # Relate expects a string of collections or None
        collections = collections if args.collections else None
        Workflow(RELATE).start_new({"catalogue": args.catalogue, "collections": collections})

    def export_test_command(self):
        parser = argparse.ArgumentParser(description='Test of exports for a catalog')
        parser.add_argument('catalogue',
                            type=str,
                            help='the name of the data catalog (example: "meetbouten"')
        # Skip the first argument
        args = parser.parse_args(sys.argv[2:])
        print(f"Trigger export test for {args.catalogue}")
        Workflow(EXPORT, EXPORT_TEST).start_new({"catalogue": args.catalogue})


def init():
    if __name__ == '__main__':
        connect()
        WorkflowCommands()


init()
