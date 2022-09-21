import os

from gobcore.logging.logger import StdoutHandler, RequestsHandler

LOG_NAME = "WORKFLOW"
LOG_HANDLERS = [RequestsHandler(), StdoutHandler()]

GOB_MGMT_DB = {
    'drivername': 'postgresql',
    'username': os.getenv("DATABASE_USER", "gob"),
    'password': os.getenv("DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("DATABASE_PORT_OVERRIDE", 5407),
    'database': os.getenv("DATABASE", 'gob_management'),
}

API_HOST = os.getenv('API_HOST', 'http://localhost:8141')
