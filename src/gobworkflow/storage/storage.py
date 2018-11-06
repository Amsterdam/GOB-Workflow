"""Storage

This module encapsulates the GOB Management storage.
"""
import datetime
import json

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from gobcore.typesystem import get_gob_type
from gobcore.typesystem.json import GobTypeJSONEncoder

from gobworkflow.config import GOB_MGMT_DB

# Ths session and Base will be initialised by the _init() method
# The _init() method is called at the end of this module
session = None
Base = automap_base()
Log = None
Service = None
ServiceTask = None
engine = None

LOG_TABLE = 'logs'
LOG_MODEL = {
    "logid": "GOB.PKInteger",   # Unique identification of the event, numbered sequentially
    "timestamp": "GOB.DateTime",
    "process_id": "GOB.String",
    "source": "GOB.String",
    "destination": "GOB.String",
    "catalogue": "GOB.String",
    "entity": "GOB.String",
    "level": "GOB.String",
    "id": "GOB.String",
    "name": "GOB.String",
    "msg": "GOB.String",
    "data": "GOB.JSON",
}

SERVICE_TABLE = 'services'
SERVICE_MODEL = {
    "id": "GOB.PKInteger",
    "name": "GOB.String",
    "is_alive": "GOB.Boolean",
    "timestamp": "GOB.DateTime"
}

SERVICE_TASK_TABLE = "service_tasks"
SERVICE_TASK_MODEL = {
    "id": "GOB.PKInteger",
    "service_name": "GOB.String",
    "name": "GOB.String",
    "is_alive": "GOB.Boolean"
}


def get_column(column):
    """Get the SQLAlchemy columndefinition for the gob type as exposed by the gob_type"""
    (column_name, gob_type_name) = column

    gob_type = get_gob_type(gob_type_name)
    return gob_type.get_column_definition(column_name)


def _create_tables(engine):
    """Create tables

    Creates tables for log, service and service tasks
    Only creates when it does not yet exist

    :param engine:
    :return: None
    """
    meta = MetaData(engine)
    for entity_model, entity_table in ((LOG_MODEL, LOG_TABLE),
                                       (SERVICE_MODEL, SERVICE_TABLE),
                                       (SERVICE_TASK_MODEL, SERVICE_TASK_TABLE)):
        print("Create", entity_model, entity_table)
        columns = [get_column(column) for column in entity_model.items()]
        table = Table(entity_table, meta, *columns, extend_existing=True)
        table.create(engine, checkfirst=True)


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, Base, engine, Log, Service, ServiceTask

    engine = create_engine(URL(**GOB_MGMT_DB))

    _create_tables(engine)

    # Reflect the database to generate classes for ORM
    Base.prepare(engine, reflect=True)

    # Get the corresponding classes
    Log = Base.classes.logs
    Service = Base.classes.services
    ServiceTask = Base.classes.service_tasks

    session = Session(engine)


def drop_tables():
    global engine, Base

    for table in [LOG_TABLE, SERVICE_TABLE, SERVICE_TASK_TABLE]:
        statement = f"DROP TABLE IF EXISTS {table} CASCADE"
        engine.execute(statement)

    # Update the reflected base
    Base = automap_base()


def save_log(msg):
    global Log, session

    # Encode the json data
    json_data = json.dumps(msg.get('data', None), cls=GobTypeJSONEncoder)

    # Create the log record
    record = Log(
        timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%f'),
        process_id=msg.get('process_id', None),
        source=msg.get('source', None),
        entity=msg.get('entity', None),
        level=msg.get('level', None),
        name=msg.get('name', None),
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
    global Service, ServiceTask, session

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
