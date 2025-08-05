import json
import os

from kafka import KafkaProducer
from dataclasses import asdict, dataclass

from celery import Task, shared_task
from celery.utils.log import get_logger

from conductor_celery.utils import configure_runner, configure_env
from conductor_celery.utils import update_task as real_update_task



logger = get_logger(__name__)

# load env
configure_env()

# caso não carregue a env, usar a config default dockercontainer kafka:9093
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9093")
KAFKA_TOPIC_FAIL = os.getenv("KAFKA_TOPIC_FAIL", "workspace_conductor_task_failures")

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

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

    def before_start(self, task_id, args, kwargs):
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

        server_api_url = self.app.conf["conductor_server_api_url"]
        logger.debug(f"ConductorTask configure_runner: {server_api_url}")
        self.runner = configure_runner(server_api_url=server_api_url, name=self.name, debug=False)

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
                logger.info(f"Task {self.name}[{task_id}] received.")
                logger.info("ConductorTask: %s", conductor_task.task_id)

    def on_success(self, retval, task_id, args, kwargs):
        if "conductor" not in self.request.headers:
            return

        logger.info(f"{self.name} {task_id} on_success: update_task: {self.request.headers} {retval} start.")

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
        logger.info(
            f'{self.name} {task_id} on_success: update_task: {self.request.headers["conductor"]["task_id"]} done.'
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Method called when a task fails. Updates the task status in Conductor with the error message.

        Args:
            exc (Exception): The exception that caused the task to fail.
            task_id (str): The ID of the failed task.
            args (list): The arguments passed to the failed task.
            kwargs (dict): The keyword arguments passed to the failed task.
            einfo (ExceptionInfo): Information about the exception that caused the task to fail.
        """
        if "conductor" not in self.request.headers:
            return

        logger.info(f"{self.name} {task_id} on_failure: update_task: {self.request.headers} start.")

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

        try:
            producer = get_kafka_producer()
            if producer:
                event = {
                    "task_name": self.name,
                    "task_id": task_id,
                    "workflow_instance_id": conductor_task.workflow_instance_id,
                    "worker_id": conductor_task.worker_id,
                    "error": str(exc),
                    "args": args,
                    "kwargs": kwargs,
                }
                producer.send(KAFKA_TOPIC_FAIL, event)
                producer.flush()
                logger.info(f"Sent failure event to Kafka topic '{KAFKA_TOPIC_FAIL}': {event}")
            else:
                logger.warning("KafkaProducer not available, skipping Kafka event send.")
        except Exception as kafka_exc:
            logger.error(f"Failed to send Kafka event: {kafka_exc}")

        
        logger.info(
            f'{self.name} {task_id} on_failure: update_task: {self.request.headers["conductor"]["task_id"]} done.'
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
