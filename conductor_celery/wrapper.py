from typing import Any

from conductor.client.automator import task_runner
from conductor.client.http.models.task import Task
from conductor.client.worker.worker_interface import WorkerInterface


class Worker(WorkerInterface):
    def __init__(self, task_definition_name) -> None:
        self.task_definition_name = task_definition_name

    def execute(self) -> None:
        pass

    def paused(self) -> bool:
        return False


class TaskRunner(task_runner.TaskRunner):
    def poll_task(self) -> Task:
        return self.__poll_task()

    def update_task(self, retval: Any) -> Any:
        return self.__update_task(retval)
