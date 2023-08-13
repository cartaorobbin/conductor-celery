import json

import httpretty

from conductor_celery.tasks import ConductorTask

httpretty.enable(allow_net_connect=False, verbose=True)


def test_conductor_task_case_1(celery_app, celery_worker, task_poll_response):
    httpretty.register_uri(
        httpretty.GET,
        "https://localhost:8080/api/tasks/poll/celery_test_task?workerid=localhost",
        body=task_poll_response({"x": 2, "y": 4}),
    )

    httpretty.register_uri(httpretty.POST, "https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 8}

    assert httpretty.latest_requests()[-1].url == "https://localhost:8080/api/tasks"
    body = json.loads(httpretty.latest_requests()[-1].body)

    assert body["outputData"] == {"total": 8}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == "localhost"

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_case_2(celery_app, celery_worker, task_poll_response):
    httpretty.register_uri(
        httpretty.GET,
        "https://localhost:8080/api/tasks/poll/celery_test_task?workerid=localhost",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    httpretty.register_uri(httpretty.POST, "https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 12}

    assert httpretty.latest_requests()[-1].url == "https://localhost:8080/api/tasks"
    body = json.loads(httpretty.latest_requests()[-1].body)

    assert body["outputData"] == {"total": 12}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == "localhost"

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]
