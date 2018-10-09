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
engine = None

LOG_TABLE = 'logs'
LOG_MODEL = {
    "logid": "GOB.PKInteger",   # Unique identification of the event, numbered sequentially
    "timestamp": "GOB.DateTime",
    "process_id": "GOB.String",
    "source": "GOB.String",
    "entity": "GOB.String",
    "level": "GOB.String",
    "name": "GOB.String",
    "msg": "GOB.String",
    "data": "GOB.JSON",
}


def get_column(column):
    """Get the SQLAlchemy columndefinition for the gob type as exposed by the gob_type"""
    (column_name, gob_type_name) = column

    gob_type = get_gob_type(gob_type_name)
    return gob_type.get_column_definition(column_name)


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, Base, engine, Log

    engine = create_engine(URL(**GOB_MGMT_DB))

    # Create the database table for logs if it doesn't exist
    meta = MetaData(engine)
    columns = [get_column(column) for column in LOG_MODEL.items()]
    table = Table(LOG_TABLE, meta, *columns, extend_existing=True)
    table.create(engine, checkfirst=True)

    # Reflect the database to generate classes for ORM
    Base.prepare(engine, reflect=True)

    # Get the log class
    Log = Base.classes.logs

    session = Session(engine)


def save_log(msg):
    global Log, session

    # Encode the json data
    json_data = json.dumps(msg.get('data', None), cls=GobTypeJSONEncoder)

    # Create the log record
    record = Log(
        timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S'),
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
