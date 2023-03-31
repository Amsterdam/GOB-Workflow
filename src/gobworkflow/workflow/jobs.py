"""
Job and JobStep functions

Used to create and update Jobs and JobSteps
"""
import datetime

from gobcore.status.heartbeat import STATUS_END, STATUS_FAIL, STATUS_SCHEDULED, STATUS_START

from gobworkflow.storage.storage import job_save, job_update, step_save, step_update


def _timestamp():
    """
    Job and job steps are given UTC timestamps
    :return: The current UTC date time
    """
    return datetime.datetime.utcnow()


def job_start(job_type, msg):
    """
    Register the start of a job

    Assign a name and register the parameters, including the start time
    :param msg: The job start parameters
    :return:
    """
    timestamp = _timestamp()
    # Concatenate all the non-header fields
    args = [str(val) for key, val in msg.get("header", {}).items() if key not in ["workflow"]]

    job_name = f"{job_type}.{'.'.join(args)}"
    start_timestamp = int(_timestamp().replace(microsecond=0).timestamp())
    process_id = msg.get("header", {}).get("process_id", f"{start_timestamp}.{job_name}")

    job_info = {
        "name": job_name,
        "process_id": process_id,
        "type": job_type,
        "args": args,
        "start": timestamp,
        "end": None,
        "status": STATUS_START,
        "user": msg.get("header", {}).get("user"),
        "catalogue": msg.get("header", {}).get("catalogue"),
        "collection": msg.get("header", {}).get("collection"),
        "attribute": msg.get("header", {}).get("attribute"),
        "application": msg.get("header", {}).get("application"),
    }
    job = job_save(job_info)
    # Store the job and register its id
    job_info["id"] = job.id
    # Enhance the message header with the job id and process_id
    msg["header"]["jobid"] = job.id
    msg["header"]["process_id"] = process_id
    return job_info


def job_end(id, status=STATUS_END):
    """
    End a job

    Register the end time and the status
    :param header: The header of the message that ended the job
    :return:
    """
    if id is None:
        return
    timestamp = _timestamp()
    job_info = {"id": id, "end": timestamp, "status": status}
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
        "status": STATUS_SCHEDULED,
    }
    step = step_save(step_info)
    # Store the step and register its id
    step_info["id"] = step.id
    # Enhance the message with the job id
    header["stepid"] = step.id
    return step_info


def step_status(jobid, stepid, status):
    """
    Register the status of a workflow step

    STATUS_START sets the start time
    Other statusses (STATUS_OK, STATUS_FAIL) set the end time

    If the step has crashed, end the worklow job
    :param jobid:
    :param stepid:
    :param status:
    :return:
    """
    timestamp = _timestamp()
    start_end = "start" if status == STATUS_START else "end"
    step_info = {"id": stepid, "status": status, start_end: timestamp}
    step_info = step_update(step_info)
    if status == STATUS_FAIL:
        job_end(jobid)
    return step_info
