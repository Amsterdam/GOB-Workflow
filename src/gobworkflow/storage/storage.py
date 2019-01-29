"""Storage

This module encapsulates the GOB Management storage.
"""
import datetime
import json

import alembic.config

from sqlalchemy import create_engine, or_
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
        application=msg.get('application', None),
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


def get_services():
    """Get services

    :return: All current services (alive or dead)
    """
    return session.query(Service).all()


def mark_service_dead(service):
    """Mark a service as not being alive anymore

    :param service: the service to mark as dead
    :return: None
    """
    # mark as dead
    service.is_alive = False
    # remove any tasks
    _update_tasks(service, [])

    session.commit()


def remove_service(service):
    """Remove a service

    :param service: the servie to remove
    :return: None
    """
    # remove any tasks
    _update_tasks(service, [])
    # remove service
    session.query(Service) \
        .filter(Service.host == service.host) \
        .filter(Service.name == service.name) \
        .delete()

    session.commit()


def update_service(service, tasks):
    """Update service state in storage

    :param service:
    :param tasks:
    :return: None
    """
    # Get the current service or create it if not yet exists
    current = session.query(Service)\
        .filter(or_(Service.host == service["host"], Service.host == None))\
        .filter(Service.name == service["name"])\
        .first()  # noqa: E711

    if current:
        current.is_alive = service["is_alive"]
        current.timestamp = service["timestamp"]
        current.host = service["host"]
        current.pid = service["pid"]
    else:
        current = Service(**service)
        session.add(current)
        session.commit()

    # Update status with current tasks
    _update_tasks(current, tasks)

    session.commit()


def _update_tasks(service, tasks):
    """Update tasks

    Delete all current tasks that are not in tasks
    Update or add all taska with the current status

    :param current_tasks:
    :param tasks:
    :return:
    """
    # Remove any dangling tasks
    session.query(ServiceTask).filter(ServiceTask.service_id == None).delete()  # noqa: E711

    # Get currently registered tasks for the service
    current_tasks = session.query(ServiceTask).filter(ServiceTask.service_id == service.id).all()

    # Delete tasks that have ended
    for task in current_tasks:
        matches = [t for t in tasks if t["name"] == task.name]
        if len(matches) == 0:
            session.delete(task)

    # Add or update tasks that are active
    for task in tasks:
        matches = [t for t in current_tasks if t.name == task["name"]]
        if len(matches) == 0:
            task = ServiceTask(**task)
            task.service_id = service.id
            session.add(task)
        else:
            matches[0].is_alive = task["is_alive"]

    # Commit is done by the caller
