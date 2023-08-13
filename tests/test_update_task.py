import json

import httpretty

from conductor_celery.tasks import update_task
from conductor_celery.utils import configure_runner

httpretty.enable(allow_net_connect=False, verbose=True)


def test_update_task(celery_app, celery_worker, task_poll_response):
    httpretty.register_uri(
        httpretty.GET,
        "https://localhost:8080/api/tasks/poll/celery_test_task?workerid=localhost",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    httpretty.register_uri(httpretty.POST, "https://localhost:8080/api/tasks")

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

    assert httpretty.latest_requests()[-1].url == "https://localhost:8080/api/tasks"
    body = json.loads(httpretty.latest_requests()[-1].body)

    assert body["outputData"] == {"total": 8}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == "localhost"

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]
