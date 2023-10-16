import json


from conductor_celery.tasks import update_task
from conductor_celery.utils import configure_runner
import socket


def test_update_task(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_test_task?workerid={worker_id}",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks", body="1233444")

    name = "celery_test_task"

    runner = configure_runner(server_api_url=celery_app.conf["conductor_server_api_url"], name=name, debug=True)

    conductor_task = runner.poll_task()

    payload = {
        "name": name,
        "task_id": conductor_task.task_id,
        "workflow_instance_id": conductor_task.workflow_instance_id,
        "worker_id": conductor_task.worker_id,
        "values": {"total": 8},
    }

    update_task.apply(kwargs=payload).get()

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 8}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]
