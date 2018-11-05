"""Heartbeats

Use heartbeats to monitor the status of the components and their tasks

The status is stored in both memory and storage

On every heartbeat all currently known services are checked for heartbeat interval timeout

The memory storage is used to compare the status with the last registered status
If the status has changed the change is written to the storage


"""
import datetime

from gobcore.status.heartbeat import HEARTBEAT_INTERVAL
from gobworkflow.storage import update_service

_ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
_SERVICE = {}
_TASKS = {}


def on_heartbeat(msg):
    """On heartbeat message

    Register the current status
    Store the status
    Check all services for timeout on heartbeat interval

    :param msg: heartbeat message
    :return: None
    """
    service_name = msg["name"]

    service = {
        "name": service_name,
        "is_alive": msg["is_alive"],
        "timestamp": msg["timestamp"]
    }

    service_tasks = [{
        "service_name": service_name,
        "name": thread["name"],
        "is_alive": thread["is_alive"]
    } for thread in msg["threads"]] if service["is_alive"] else []

    # Register in memory
    _SERVICE[service_name] = service
    _TASKS[service_name] = service_tasks

    # Update in storage
    update_service(service, service_tasks)

    # timeout of heartbeat interval check
    check_services()


def check_services():
    """Check services on heartbeat timeout

    If a heartbeat has not been received in the heartbeat timeour interval mark the process as dead

    :return: None
    """
    now = datetime.datetime.now()
    for service_name, service in _SERVICE.items():
        # Only check services that are currently marked as alive
        if service["is_alive"]:
            last_heartbeat = datetime.datetime.strptime(service["timestamp"], _ISOFORMAT)
            time_ago = now - last_heartbeat
            if time_ago.total_seconds() > HEARTBEAT_INTERVAL:
                # Heartbeat timeout, register as dead
                service["is_alive"] = False
                _TASKS[service_name] = []
                # Update in storage
                update_service(service, [])
