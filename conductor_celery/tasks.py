import logging

from celery import Task, shared_task

from conductor_celery.utils import configure_runner
from conductor_celery.utils import update_task as real_update_task

logger = logging.getLogger(__name__)


class ConductorPollTask(Task):
    pass


class ConductorTask(Task):
    """
    This handle a canductor task
    """

    def __call__(self, *args, **kwargs):
        server_api_url = self.app.conf["conductor_server_api_url"]
        logger.info(f"ConductorTask configure_runner: ${server_api_url}")

        runner = configure_runner(server_api_url=server_api_url, name=self.name, debug=True)

        conductor_task = runner.poll_task()

        ret = self.run(**conductor_task.input_data)

        runner.update_task(
            real_update_task(conductor_task.task_id, conductor_task.workflow_instance_id, conductor_task.worker_id, ret)
        )
        return ret


@shared_task(bind=True)
def update_task(self, name, task_id, workflow_instance_id, worker_id, values):
    runner = configure_runner(server_api_url=self.app.conf["conductor_server_api_url"], name=name, debug=True)
    runner.update_task(real_update_task(task_id, workflow_instance_id, worker_id, values))
