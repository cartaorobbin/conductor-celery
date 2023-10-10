from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.metrics_settings import MetricsSettings
from conductor.client.http.models.task_result import TaskResult

from conductor_celery.wrapper import TaskRunner, Worker


def configure_runner(server_api_url: str, name: str, debug=False):
    configuration = Configuration(server_api_url=server_api_url, debug=True)

    worker = Worker(name)
    metrics_settings = MetricsSettings()
    return TaskRunner(worker, configuration, metrics_settings)


def update_task(task_id, workflow_instance_id, worker_id, values, status) -> TaskResult:
    task_result = TaskResult(
        task_id=task_id,
        workflow_instance_id=workflow_instance_id,
        worker_id=worker_id,
    )
    for key, value in values.items():
        task_result.add_output_data(key, value)

    task_result.status = status
    return task_result
