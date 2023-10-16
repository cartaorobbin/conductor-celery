import json
import socket

# import httpretty

from conductor_celery.tasks import ConductorTask

# httpretty.enable(allow_net_connect=False, verbose=True)


def test_conductor_task_case_1(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_test_task?workerid={worker_id}",
        body=task_poll_response({"x": 2, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply([2, 2]).result == {"total": 8}

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 8}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_case_2(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_test_task?workerid={worker_id}",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply([2, 2]).result == {"total": 12}

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 12}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_case_3(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_test_task?workerid={worker_id}",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 12}

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 12}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_task_name(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "task_name"

    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name)
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 12}

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 12}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_bind_true(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "task_name"

    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"x": 3, "y": 4}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name, bind=True)
    def mul(self, x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 12}

    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"total": 12}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_other_signatures(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "task_name"

    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"company_id": "3"}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name, bind=True)
    def mul(self, company_id):
        return {"company": company_id}

    celery_worker.reload()
    assert mul.apply().result == {"company": "3"}

    assert len(responses.calls) == 2
    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"company": "3"}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_delay(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "task_name"

    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"company_id": "3"}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name, bind=True)
    def mul(self, company_id):
        return {"company": company_id}

    celery_worker.reload()
    assert mul.delay(2).get() == {"company": "3"}

    assert len(responses.calls) == 2
    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"company": "3"}
    assert body["status"] == "COMPLETED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_error_with_auto_retry(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "celery_test_task"
    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"x": 2, "y": 0}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name, autoretry_for=(ZeroDivisionError,))
    def div(x, y):
        return {"total": x / y}

    celery_worker.reload()
    assert div.apply([2, 0]).result.__class__ == ZeroDivisionError

    assert len(responses.calls) == 2
    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"error": "division by zero"}
    assert body["status"] == "FAILED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]


def test_conductor_task_error_without_auto_retry(celery_app, celery_worker, task_poll_response, responses):
    worker_id = socket.gethostname()
    my_task_name = "celery_test_task"
    responses.get(
        f"https://localhost:8080/api/tasks/poll/{my_task_name}?workerid={worker_id}",
        body=task_poll_response({"x": 2, "y": 0}),
    )

    responses.post("https://localhost:8080/api/tasks")

    @celery_app.task(base=ConductorTask, name=my_task_name)
    def div(x, y):
        return {"total": x / y}

    celery_worker.reload()
    assert div.apply([2, 0]).result.__class__ == ZeroDivisionError

    assert len(responses.calls) == 2
    assert responses.calls[-1].request.url == "https://localhost:8080/api/tasks"
    body = json.loads(responses.calls[-1].request.body)

    assert body["outputData"] == {"error": "division by zero"}
    assert body["status"] == "FAILED"

    assert body["workflowInstanceId"] == "18bfeabf-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["taskId"] == "18c0fc30-3a1c-11ee-868b-06fd3bd0ae8b"
    assert body["workerId"] == worker_id

    assert list(body.keys()) == ["workflowInstanceId", "taskId", "workerId", "status", "outputData"]
