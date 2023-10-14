import logging
from dataclasses import asdict, dataclass

from celery import Task, shared_task

from conductor_celery.utils import configure_runner
from conductor_celery.utils import update_task as real_update_task

logger = logging.getLogger(__name__)


class ConductorPollTask(Task):
    pass


@dataclass
class PooledConductorTask:
    task_id: str
    workflow_instance_id: str
    worker_id: str
    input_data: dict


class ConductorTask(Task):
    """
    This handle a canductor task
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        server_api_url = self.app.conf["conductor_server_api_url"]
        logger.debug(f"ConductorTask configure_runner: {server_api_url}")
        self.runner = configure_runner(server_api_url=server_api_url, name=self.name, debug=True)

    def before_start(self, task_id: str, args: tuple, kwargs: dict) -> None:
        """
        Method called before a task is started. It sets the headers for the request and
        updates the kwargs and args based on the Conductor task information.

        Args:
            task_id (str): The ID of the task to be started.
            args (Tuple): The arguments to be passed to the task.
            kwargs (Dict): The keyword arguments to be passed to the task.

        Returns:
            None
        """
        if not self.request.headers:
            self.request.headers = {}
        if "conductor" not in self.request.headers:
            conductor_task = self.runner.poll_task()
            if conductor_task.task_id:
                self.request.headers["conductor"] = asdict(
                    PooledConductorTask(
                        input_data=conductor_task.input_data,
                        task_id=conductor_task.task_id,
                        workflow_instance_id=conductor_task.workflow_instance_id,
                        worker_id=conductor_task.worker_id,
                    )
                )
                self.request.kwargs = conductor_task.input_data
                self.request.args = []

    def on_success(self, retval, task_id, args, kwargs):
        if "conductor" not in self.request.headers:
            return

        conductor_task = PooledConductorTask(**self.request.headers["conductor"])
        self.runner.update_task(
            real_update_task(
                conductor_task.task_id,
                conductor_task.workflow_instance_id,
                conductor_task.worker_id,
                retval,
                "COMPLETED",
            )
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if "conductor" not in self.request.headers:
            return

        conductor_task = PooledConductorTask(**self.request.headers["conductor"])
        self.runner.update_task(
            real_update_task(
                conductor_task.task_id,
                conductor_task.workflow_instance_id,
                conductor_task.worker_id,
                {"error": str(exc)},
                "FAILED",
            )
        )

    def __call__(self, *args, **kwargs):
        """
        A task can be called like a regular function. But use conductor args and kwargs
        """
        if "conductor" not in self.request.headers:
            return

        return self.run(**self.request.kwargs)


@shared_task(bind=True)
def update_task(self, name, task_id, workflow_instance_id, worker_id, values):
    runner = configure_runner(server_api_url=self.app.conf["conductor_server_api_url"], name=name, debug=True)
    runner.update_task(real_update_task(task_id, workflow_instance_id, worker_id, values, "COMPLETED"))
