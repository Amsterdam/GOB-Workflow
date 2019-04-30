"""
Job and JobStep functions

Used to create and update Jobs and JobSteps
"""
import datetime

from gobcore.status.heartbeat import STATUS_START
from gobworkflow.storage.storage import job_save, job_update, step_save, step_update


def _timestamp():
    """
    Job and job steps are given UTC timestamps
    :return: The current UTC date time
    """
    return datetime.datetime.utcnow()


def job_start(job_name, msg):
    """
    Register the start of a job

    Assign a name and register the parameters, including the start time
    :param msg: The job start parameters
    :return:
    """
    timestamp = _timestamp()
    # Concatenate all the non-header fields
    args = [str(val) for key, val in msg.items() if key != "header"]
    job_info = {
        "name": f"{job_name}.{'.'.join(args)}",
        "type": job_name,
        "args": args,
        "start": timestamp,
        "end": None,
        "status": "started"
    }
    job = job_save(job_info)
    # Store the job and register its id
    job_info["id"] = job.id
    # Enhance the message header with the job id
    msg["header"]["jobid"] = job.id
    return job_info


def job_end(header):
    """
    End a job

    Register the end time and the status
    :param header: The header of the message that ended the job
    :return:
    """
    id = header.get("jobid")
    if id is None:
        return
    timestamp = _timestamp()
    job_info = {
        "id": id,
        "end": timestamp,
        "status": "ended"
    }
    job_update(job_info)
    return job_info


def step_start(step_name, header):
    """
    Start a job step

    Register its name, mark it as started and register the start time
    :param step_name: The name of the step
    :param header: The header of the message that started the step
    :return:
    """
    step_info = {
        "jobid": header.get("jobid"),
        "name": step_name,
        "start": None,
        "end": None,
        "status": "scheduled"
    }
    step = step_save(step_info)
    # Store the step and register its id
    step_info["id"] = step.id
    # Enhance the message with the job id
    header["stepid"] = step.id
    return step_info


def step_status(id, status):
    """
    Register the status of a workflow step

    STATUS_START sets the start time
    Other statusses (STATUS_OK, STATUS_FAIL) set the end time
    :param id:
    :param status:
    :return:
    """
    timestamp = _timestamp()
    start_end = "start" if status == STATUS_START else "end"
    step_info = {
        "id": id,
        "status": status,
        start_end: timestamp
    }
    step_update(step_info)
    return step_info
