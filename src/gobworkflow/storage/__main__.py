"""Storage commands

This command line script can be used to clear the database or truncate the
tables. For now this is intented to reduce the manual steps needed when
changing models or other database tables.

     python -m gobupload.storage resetdb
"""
import argparse
import getpass

from gobworkflow.config import GOB_MGMT_DB
from gobworkflow.storage.storage import connect, drop_tables


parser = argparse.ArgumentParser(
    prog='python -m gobworkflow.storage',
    description='Perform database mutations',
    epilog='Generieke Ontsluiting Basisregistraties')

command = parser.add_subparsers(title='the command to perform',
                                dest='command',
                                metavar='command')
command.required = True
command.add_parser('resetdb', help="reset the database to it's empty state")

args = parser.parse_args()

confirm = getpass.getpass("""You have requested a reset of the database.
This will IRREVERSIBLY DESTROY all data currently in the GOB database,
and return each table to an empty state.
Are you sure you want to do this?
    Type the database password to continue, or 'no' to cancel: """)

if confirm == GOB_MGMT_DB['password']:
    connect()

    if args.command == 'resetdb':
        # Drop all tables and re-initialize the database
        drop_tables()
        connect()
else:
    print("Database reset cancelled.")
