"""Heartbeats

Use heartbeats to monitor the status of the components and their tasks

The status is stored in both memory and storage

On every heartbeat all currently known services are checked for heartbeat interval timeout

The memory storage is used to compare the status with the last registered status
If the status has changed the change is written to the storage


"""
import datetime

from sqlalchemy.orm.exc import ObjectDeletedError

from gobcore.status.heartbeat import HEARTBEAT_INTERVAL
from gobworkflow.storage.storage import update_service, remove_service, get_services, mark_service_dead

# Remove a service after not having received anything for SERVICE_REMOVAL_TIMEOUT seconds
_SERVICE_REMOVAL_TIMEOUT = HEARTBEAT_INTERVAL * 60


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
        "host": msg.get("host"),
        "pid": msg.get("pid"),
        "is_alive": msg["is_alive"],
        "timestamp": msg["timestamp"]
    }

    service_tasks = {
        thread['name']: {
            'service_name': service_name,
            'name': thread['name'],
            'is_alive': thread['is_alive']
        }
        for thread in msg['threads']
    } if service['is_alive'] else {}

    # Update in storage
    update_service(service, service_tasks.values())

    # timeout of heartbeat interval check
    check_services()


def check_services():
    """Check services on heartbeat timeout

    If a heartbeat has not been received in the heartbeat timeour interval mark the process as dead

    :return: None
    """
    now = datetime.datetime.utcnow()
    for service in get_services():
        try:
            # Only check services that are currently marked as alive
            last_heartbeat = service.timestamp
        except ObjectDeletedError as err:
            print("Service expired: ", str(err))
        else:
            time_ago = now - last_heartbeat
            if time_ago.total_seconds() > _SERVICE_REMOVAL_TIMEOUT:
                # Final timeout reached, remove the service
                remove_service(service)
            elif time_ago.total_seconds() > (HEARTBEAT_INTERVAL * 2):
                # Heartbeat timeout, register as dead
                mark_service_dead(service)
