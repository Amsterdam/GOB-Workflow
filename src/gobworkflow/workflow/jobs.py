"""
Job and JobStep functions

Used to create and update Jobs and JobSteps
"""
import datetime

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
    timestamp = _timestamp()
    step_info = {
        "jobid": header.get("jobid"),
        "name": step_name,
        "start": timestamp,
        "end": None,
        "status": "started"
    }
    step = step_save(step_info)
    # Store the step and register its id
    step_info["id"] = step.id
    # Enhance the message with the job id
    header["stepid"] = step.id
    return step_info


def step_end(header):
    """
    End a job step

    Register its status and end time
    :param header: The header of the message that ended the step
    """
    id = header.get("stepid")
    if id is None:
        return
    timestamp = _timestamp()
    step_info = {
        "id": id,
        "end": timestamp,
        "status": "ended"
    }
    step_update(step_info)
    return step_info
