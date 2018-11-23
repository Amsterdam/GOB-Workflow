"""Storage

This module encapsulates the GOB Management storage.
"""
import datetime
import json

import alembic.config

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobcore.typesystem.json import GobTypeJSONEncoder

from gobcore.model.sa.management import Base, Log, Service, ServiceTask

from gobworkflow.config import GOB_MGMT_DB

session = None
engine = None


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, engine

    # Database migrations are handled by alembic
    # alembic upgrade head
    alembicArgs = [
        '--raiseerr',
        'upgrade', 'head',
    ]
    alembic.config.main(argv=alembicArgs)

    engine = create_engine(URL(**GOB_MGMT_DB))

    # Declarative base model to create database tables and classes
    Base.metadata.bind = engine

    session = Session(engine)


def save_log(msg):
    # Encode the json data
    json_data = json.dumps(msg.get('data', None), cls=GobTypeJSONEncoder)

    # Create the log record
    record = Log(
        timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%f'),
        process_id=msg.get('process_id', None),
        source=msg.get('source', None),
        destination=msg.get('destination', None),
        catalogue=msg.get('catalogue', None),
        entity=msg.get('entity', None),
        level=msg.get('level', None),
        name=msg.get('name', None),
        msgid=msg.get('id', None),
        msg=msg.get('msg', None),
        data=json_data,
    )
    session.add(record)
    session.commit()


def update_service(service, tasks):
    """Update service state in storage

    :param service:
    :param tasks:
    :return: None
    """
    # Get the current service or create it if not yet exists
    current = session.query(Service).filter_by(name=service["name"]).first()
    if current:
        current.is_alive = service["is_alive"]
        current.timestamp = service["timestamp"]
    else:
        session.add(Service(**service))

    # Get currently registered tasks
    current_tasks = session.query(ServiceTask).filter_by(service_name=service["name"]).all()

    # Update status with current tasks
    _update_tasks(current_tasks, tasks)

    session.commit()


def _update_tasks(current_tasks, tasks):
    """Update tasks

    Delete all current tasks that are not in tasks
    Update or add all taska with the current status

    :param current_tasks:
    :param tasks:
    :return:
    """
    for task in current_tasks:
        matches = [t for t in tasks if t["name"] == task.name]
        if len(matches) == 0:
            session.delete(task)

    for task in tasks:
        matches = [t for t in current_tasks if t.name == task["name"]]
        if len(matches) == 0:
            session.add(ServiceTask(**task))
        else:
            matches[0].is_alive = task["is_alive"]
