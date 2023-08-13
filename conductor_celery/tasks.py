from typing import Any

from celery import Task
from conductor.client.automator import task_runner
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.metrics_settings import MetricsSettings
from conductor.client.http.models.task import Task as _ConductorTask
from conductor.client.http.models.task_result import TaskResult
from conductor.client.worker.worker_interface import WorkerInterface


class Worker(WorkerInterface):
    def __init__(self, task_definition_name) -> None:
        self.task_definition_name = task_definition_name

    def execute(self) -> None:
        pass

    def paused(self) -> bool:
        return False


class TaskRunner(task_runner.TaskRunner):
    def poll_task(self) -> _ConductorTask:
        return self.__poll_task()

    def update_task(self, retval: Any) -> Any:
        return self.__update_task(retval)


class ConductorTask(Task):
    """
    This handle a canductor task
    """

    def _conductor_before_start(self) -> None:
        configuration = Configuration(server_api_url="https://localhost:8080/api", debug=True)

        self.worker = Worker(self.name)
        metrics_settings = MetricsSettings()
        self.runner = TaskRunner(self.worker, configuration, metrics_settings)

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
        self._conductor_before_start()
        conductor_task = self.runner.poll_task()
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

        task_result = TaskResult(
            task_id=conductor_task.task_id,
            workflow_instance_id=conductor_task.workflow_instance_id,
            worker_id=self.worker.task_definition_name,
        )
        for key, value in ret.result.items():
            task_result.add_output_data(key, value)

        task_result.status = "COMPLETED"

        self.runner.update_task(task_result)
        return ret
