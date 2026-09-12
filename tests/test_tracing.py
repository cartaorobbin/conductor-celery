import socket

import pytest
from opentelemetry import trace
from opentelemetry.propagate import inject

from conductor_celery.tasks import ConductorTask
from conductor_celery.tracing import (
    CONDUCTOR_SPAN_NAME,
    activate_trace_context,
    deactivate_trace_context,
    split_trace_context,
    tag_current_span,
)

W3C_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


@pytest.mark.parametrize(
    ("input_data", "expected_kwargs", "expected_carrier"),
    [
        (
            {"x": 2, "y": 4, "traceparent": W3C_TRACEPARENT},
            {"x": 2, "y": 4},
            {"traceparent": W3C_TRACEPARENT},
        ),
        (
            {"x": 1, "traceparent": W3C_TRACEPARENT, "tracestate": "vendor=1"},
            {"x": 1},
            {"traceparent": W3C_TRACEPARENT, "tracestate": "vendor=1"},
        ),
        (None, {}, {}),
        ({}, {}, {}),
        ({"company_id": "3"}, {"company_id": "3"}, {}),
    ],
    ids=[
        "traceparent",
        "traceparent_and_tracestate",
        "none_input",
        "empty_input",
        "no_reserved_keys",
    ],
)
def test_split_trace_context_strips_reserved_keys(input_data, expected_kwargs, expected_carrier):
    """Reserved tracing keys leave the task kwargs and populate the carrier."""
    task_kwargs, carrier = split_trace_context(input_data)
    assert task_kwargs == expected_kwargs
    assert carrier == expected_carrier


def test_split_trace_context_does_not_mutate_input():
    """split_trace_context copies input_data before popping reserved keys."""
    input_data = {"x": 1, "traceparent": W3C_TRACEPARENT}
    split_trace_context(input_data)
    assert input_data == {"x": 1, "traceparent": W3C_TRACEPARENT}


def test_activate_trace_context_restores_parent_trace_id(tracer):
    """extract/attach makes the current span share the injected parent trace_id."""
    with tracer.start_as_current_span("starter") as span:
        parent_trace_id = span.get_span_context().trace_id
        carrier = {}
        inject(carrier)
    activate_trace_context("celery-task-1", {"traceparent": carrier["traceparent"]})
    current_trace_id = trace.get_current_span().get_span_context().trace_id
    deactivate_trace_context("celery-task-1")
    assert current_trace_id == parent_trace_id


def test_deactivate_trace_context_clears_parent(tracer):
    """After detach, the current span is no longer the injected parent."""
    with tracer.start_as_current_span("starter") as span:
        parent_trace_id = span.get_span_context().trace_id
        carrier = {}
        inject(carrier)
    activate_trace_context("celery-task-2", {"traceparent": carrier["traceparent"]})
    deactivate_trace_context("celery-task-2")
    current_trace_id = trace.get_current_span().get_span_context().trace_id
    assert current_trace_id != parent_trace_id


def test_tag_current_span_sets_searchable_attributes(tracer, finished_spans):
    """traceparent and Conductor ids are written on the current recording span."""
    with tracer.start_as_current_span("tagged"):
        tag_current_span({"traceparent": W3C_TRACEPARENT}, "conductor-task-1", "workflow-1")
    spans = finished_spans.get_finished_spans()
    assert spans[0].attributes["traceparent"] == W3C_TRACEPARENT
    assert spans[0].attributes["conductor.task_id"] == "conductor-task-1"
    assert spans[0].attributes["conductor.workflow_instance_id"] == "workflow-1"


def test_tag_current_span_after_attach_writes_recording_span(tracer, finished_spans):
    """Attributes land on the conductor.task span created after extract/attach."""
    with tracer.start_as_current_span("starter") as span:
        parent_trace_id = span.get_span_context().trace_id
        carrier = {}
        inject(carrier)
    activate_trace_context("celery-task-tag", {"traceparent": carrier["traceparent"]})
    tag_current_span({"traceparent": carrier["traceparent"]}, "conductor-task-1", "workflow-1")
    deactivate_trace_context("celery-task-tag")
    spans = finished_spans.get_finished_spans()
    assert spans[1].name == CONDUCTOR_SPAN_NAME
    assert spans[1].context.trace_id == parent_trace_id
    assert spans[1].attributes["traceparent"] == carrier["traceparent"]
    assert spans[1].attributes["conductor.task_id"] == "conductor-task-1"
    assert spans[1].attributes["conductor.workflow_instance_id"] == "workflow-1"


def test_conductor_task_strips_traceparent(celery_app, celery_worker, task_poll_response, responses):
    """traceparent in Conductor inputData is not passed to the task function."""
    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_test_task?workerid={worker_id}",
        body=task_poll_response({"x": 2, "y": 4, "traceparent": W3C_TRACEPARENT}),
    )
    responses.post("https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(base=ConductorTask, name="celery_test_task")
    def mul(x, y):
        return {"total": x * y}

    celery_worker.reload()
    assert mul.apply().result == {"total": 8}


def test_conductor_task_restores_parent_trace_id(celery_app, celery_worker, task_poll_response, responses, tracer):
    """Task body sees the same trace_id as the injected traceparent."""
    with tracer.start_as_current_span("starter") as span:
        parent_trace_id = format(span.get_span_context().trace_id, "032x")
        carrier = {}
        inject(carrier)

    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_trace_task?workerid={worker_id}",
        body=task_poll_response(
            {"x": 2, "y": 4, "traceparent": carrier["traceparent"]},
            task_name="celery_trace_task",
        ),
    )
    responses.post("https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(base=ConductorTask, name="celery_trace_task")
    def mul(x, y):
        span_context = trace.get_current_span().get_span_context()
        return {"total": x * y, "trace_id": format(span_context.trace_id, "032x")}

    celery_worker.reload()
    assert mul.apply().result == {"total": 8, "trace_id": parent_trace_id}


def test_conductor_task_keeps_parent_trace_id_on_retry(celery_app, celery_worker, task_poll_response, responses, tracer):
    """Celery retry re-attaches the stored carrier so the parent trace_id is kept."""
    with tracer.start_as_current_span("starter") as span:
        parent_trace_id = format(span.get_span_context().trace_id, "032x")
        carrier = {}
        inject(carrier)

    worker_id = socket.gethostname()
    responses.get(
        f"https://localhost:8080/api/tasks/poll/celery_retry_trace_task?workerid={worker_id}",
        body=task_poll_response(
            {"x": 2, "y": 0, "traceparent": carrier["traceparent"]},
            task_name="celery_retry_trace_task",
        ),
    )
    responses.post("https://localhost:8080/api/tasks", body="1233444")

    @celery_app.task(
        base=ConductorTask,
        name="celery_retry_trace_task",
        autoretry_for=(ValueError,),
        retry_kwargs={"max_retries": 1},
    )
    def boom(x, y):
        span_context = trace.get_current_span().get_span_context()
        raise ValueError(format(span_context.trace_id, "032x"))

    celery_worker.reload()
    result = boom.apply().result
    assert result.__class__ == ValueError
    assert result.args == (parent_trace_id,)
