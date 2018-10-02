import os

GOB_MGMT_DB = {
    'drivername': 'postgres',
    'username': os.getenv("DATABASE_USER", "gob"),
    'password': os.getenv("DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("DATABASE_PORT_OVERRIDE", 5407),
    'database': os.getenv("DATABASE", 'gob_management'),
}
