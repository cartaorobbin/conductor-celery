import httpretty

from conductor_celery.tasks import ConductorTask

httpretty.enable(allow_net_connect=False, verbose=True)


def test_create_task(celery_app, celery_worker, task_poll_response):
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
