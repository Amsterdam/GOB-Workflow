import json
from datetime import datetime
from gobworkflow.storage.storage import get_job_step, task_save, get_tasks_for_stepid, task_lock, task_unlock, \
    task_get, task_update
from gobcore.exceptions import GOBException
from gobcore.message_broker import publish
from gobcore.message_broker.offline_contents import load_message


class TaskQueue:
    """TaskQueue

    Queue that can be used by services to cut a JobStep (or other work unit, for that matter) into smaller Tasks.
    The method on_start_tasks is the entry method for the TaskQueue.

    TaskQueue makes sure tasks are executed with interdependencies in mind. The tasks are published on the dst_queue
    provided by the user, under key {key_prefix}.task, where prefix is supplied by the user. After all tasks have
    been completed, a message is sent to {key_prefix}.complete. It is the responsibility for the user of TaskQueue to
    have listeners implemented on dst_queue with keys {key_prefix}.task and {key_prefix}.complete.

    Example implementation:
    The Prepare service has a list of actions, which may depend on other actions. Prepare supplies a list of id's with
    dependencies to TaskQueue. TaskQueue creates Task objects for the given id's and only places a Task message on the
    queue when all prerequisites are fulfilled.
    After all tasks have been completed, TaskQueue puts a message on the complete queue with the combined summaries of
    all tasks.
    """
    STATUS_NEW = 'new'
    STATUS_LOCKED = 'locked'
    STATUS_COMPLETED = 'completed'
    STATUS_QUEUED = 'queued'
    STATUS_ABORTED = 'aborted'
    STATUS_FAILED = 'failed'

    TASK_KEY = ".task"
    COMPLETE_KEY = ".complete"

    def on_start_tasks(self, msg):
        """Entry method for TaskQueue. Creates tasks and puts task messages on the

        :param msg:
        :return:
        """
        header = msg['header']
        stepid = header['stepid']
        jobid = header['jobid']

        # Incoming message may be large. Manually load message from file if necessary
        msg, _ = load_message(msg, json.loads, {'stream_contents': False})

        """
        tasks: [{'id': 'some_id', 'dependencies': ['some_id', 'some_other_id']}
        """
        tasks = msg['contents']['tasks']
        dst_queue = msg['contents']['dst_queue']
        key_prefix = msg['contents']['key_prefix']
        extra_msg = msg['contents']['extra_msg']
        job, step = get_job_step(jobid, stepid)

        if not step:
            raise GOBException(f"No jobstep found with id {stepid}")

        self._validate_dependencies(tasks)
        self._create_tasks(jobid, stepid, tasks, dst_queue, key_prefix, extra_msg)
        self._queue_free_tasks_for_jobstep(stepid)

    def _validate_dependencies(self, tasks):
        """Basic validation of dependencies. Assumes tasks is already ordered.

        :param tasks:
        :return:
        """
        ids = [task['id'] for task in tasks if 'id' in task]
        assert len(set(ids)) == len(tasks), "All tasks should have a unique id"

        done = []

        for task in tasks:
            assert 'dependencies' in task

            for dependency in task['dependencies']:
                if dependency not in done:
                    raise GOBException(f"Task {task['id']} depends on task {dependency}, but isn't executed yet")

            done.append(task['id'])

    def _create_tasks(self, jobid, stepid, tasks, dst_queue, key_prefix, extra_msg):
        """Create Task objects for the input list 'tasks'.

        :param jobid:
        :param stepid:
        :param tasks:
        :param dst_queue:
        :param key_prefix:
        :param extra_msg:
        :return:
        """
        existing = get_tasks_for_stepid(stepid)
        assert len(existing) == 0, f"Already have tasks for jobstep {stepid}"

        for task in tasks:
            task_def = {
                'name': task['id'],
                'dependencies': task['dependencies'],
                'status': self.STATUS_NEW,
                'jobid': jobid,
                'stepid': stepid,
                'dst_queue': dst_queue,
                'key_prefix': key_prefix,
                'extra_msg': {
                    # Add global extra msg and extra_msg on task level
                    **extra_msg,
                    **task.get('extra_msg', {}),
                }
            }
            task_save(task_def)

    def _queue_free_tasks_for_jobstep(self, jobstep_id):
        """Queues the free tasks for jobstep.

        :param stepid:
        :return:
        """
        tasks = get_tasks_for_stepid(jobstep_id)

        completed = [task.name for task in tasks if task.status == self.STATUS_COMPLETED]
        new = [task for task in tasks if task.status == self.STATUS_NEW]
        for task in new:
            if all([dep in completed for dep in task.dependencies]):
                if task_lock(task):
                    if task_get(task.id).status == self.STATUS_NEW:
                        self._queue_task(task)
                    task_unlock(task)

    def _queue_task(self, task):
        """Queues Task object

        :param task:
        :return:
        """
        msg = {
            **task.extra_msg,
            'taskid': task.id,
            'id': task.name,
            'header': {
                'jobid': task.jobid,
                'stepid': task.stepid,
            }
        }
        publish(task.dst_queue, task.key_prefix + self.TASK_KEY, msg)

        task_update({
            'status': self.STATUS_QUEUED,
            'id': task.id,
            'start': datetime.now(),
        })

    def _all_tasks_complete(self, stepid):
        """Returns whether all tasks for given stepid have STATUS_COMPLETED

        :param stepid:
        :return:
        """
        tasks = get_tasks_for_stepid(stepid)
        completed = [task for task in tasks if task.status == self.STATUS_COMPLETED]
        return len(completed) == len(tasks)

    def on_task_result(self, msg):
        """Callback method when a Task result comes in. Handles further processing of results and triggers new
        messages.

        :param msg:
        :return:
        """
        task = task_get(msg['header']['taskid'])
        failed = len(msg['summary'].get('errors', [])) > 0

        task_info = {
            'id': task.id,
            'status': self.STATUS_FAILED if failed else self.STATUS_COMPLETED,
            'summary': msg['summary'],
            'end': datetime.now(),
        }
        task_update(task_info)

        if failed:
            self._abort_tasks(task.stepid)
        else:
            self._queue_free_tasks_for_jobstep(task.stepid)

            if self._all_tasks_complete(task.stepid):
                self._publish_complete(task)

    def _abort_tasks(self, stepid):
        """Aborts all tasks belonging to stepid, as long as they are not queued or started yet.

        :param stepid:
        :return:
        """
        all_tasks = get_tasks_for_stepid(stepid)
        new = [task for task in all_tasks if task.status == self.STATUS_NEW]

        for task in new:
            if task_lock(task):
                task_update({
                    'id': task.id,
                    'status': self.STATUS_ABORTED
                })
                task_unlock(task)

        # Finish
        self._publish_complete(all_tasks[0])

    def _publish_complete(self, task):
        """Method is triggered when all tasks in a group have completed. Also triggered when tasks are stopped
        because of failures. Handles final callback message to the user of the queue.

        :param task:
        :return:
        """
        all_tasks = get_tasks_for_stepid(task.stepid)
        warnings = [warning for sublist in [t.summary['warnings'] for t in all_tasks] for warning in sublist]
        errors = [error for sublist in [t.summary['errors'] for t in all_tasks] for error in sublist]

        msg = {
            **task.extra_msg,
            'header': {
                'jobid': task.jobid,
                'stepid': task.stepid,
            },
            'summary': {
                'warnings': warnings,
                'errors': errors,
            }
        }

        publish(task.dst_queue, task.key_prefix + self.COMPLETE_KEY, msg)
