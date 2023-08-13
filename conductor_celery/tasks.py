from typing import Any

from celery import Task, shared_task

from conductor_celery.utils import configure_runner
from conductor_celery.utils import update_task as real_update_task


class ConductorPollTask(Task):
    pass


class ConductorTask(Task):
    """
    This handle a canductor task
    """

    def apply(
        self,
        args=None,
        kwargs=None,
        link=None,
        link_error=None,
        task_id=None,
        retries=None,
        throw=None,
        logfile=None,
        loglevel=None,
        headers=None,
        **options,
    ) -> Any:
        runner = configure_runner(server_api_url=self.app.conf["conductor_server_api_url"], name=self.name, debug=True)
        conductor_task = runner.poll_task()

        ret = super().apply(
            None,
            conductor_task.input_data,
            link,
            link_error,
            task_id,
            retries,
            throw,
            logfile,
            loglevel,
            headers,
            **options,
        )

        runner.update_task(
            real_update_task(
                conductor_task.task_id, conductor_task.workflow_instance_id, conductor_task.worker_id, ret.result
            )
        )
        return ret


@shared_task(bind=True)
def update_task(self, name, task_id, workflow_instance_id, worker_id, values):
    runner = configure_runner(server_api_url=self.app.conf["conductor_server_api_url"], name=name, debug=True)
    runner.update_task(real_update_task(task_id, workflow_instance_id, worker_id, values))
