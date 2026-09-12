from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

try:
    from opentelemetry import trace
    from opentelemetry.context import attach, detach  # type: ignore[attr-defined]
    from opentelemetry.propagate import extract  # type: ignore[attr-defined]
    from opentelemetry.trace import set_span_in_context
except ImportError:
    attach = None
    detach = None
    extract = None
    set_span_in_context = None
    trace = None

RESERVED_INPUT_KEYS = ("correlation_id", "traceparent", "tracestate")
CONDUCTOR_SPAN_NAME = "conductor.task"

TRACEPARENT_RE = re.compile(
    r"^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$",
    re.IGNORECASE,
)


@dataclass
class _OtelTaskState:
    token: Any
    span: Any


_otel_state: dict[str, _OtelTaskState] = {}


def split_trace_context(input_data: dict | None) -> tuple[dict, dict]:
    """Copy input_data, strip reserved tracing keys, and build a W3C carrier."""
    task_kwargs = dict(input_data or {})
    correlation_id, traceparent, tracestate = (task_kwargs.pop(key, None) for key in RESERVED_INPUT_KEYS)

    carrier: dict = {}
    if isinstance(traceparent, str) and traceparent:
        carrier["traceparent"] = traceparent
    if isinstance(tracestate, str) and tracestate:
        carrier["tracestate"] = tracestate
    if isinstance(correlation_id, str) and correlation_id:
        carrier["correlation_id"] = correlation_id
        if TRACEPARENT_RE.match(correlation_id) and "traceparent" not in carrier:
            carrier["traceparent"] = correlation_id

    return task_kwargs, carrier


def _extract_parent_context(carrier: dict) -> Any:
    if extract is None:
        return None

    extract_carrier = {}
    if carrier.get("traceparent"):
        extract_carrier["traceparent"] = carrier["traceparent"]
    if carrier.get("tracestate"):
        extract_carrier["tracestate"] = carrier["tracestate"]
    if not extract_carrier:
        return None
    return extract(extract_carrier)


def activate_trace_context(task_id: str, carrier: dict) -> None:
    """Restore parent context and start a recording conductor.task span for this Celery task_id."""
    deactivate_trace_context(task_id)
    if trace is None:
        return

    parent_ctx = _extract_parent_context(carrier)
    span = trace.get_tracer("conductor_celery").start_span(CONDUCTOR_SPAN_NAME, context=parent_ctx)
    token = None
    if attach is not None and set_span_in_context is not None:
        token = attach(set_span_in_context(span, parent_ctx))
    _otel_state[task_id] = _OtelTaskState(token=token, span=span)


def deactivate_trace_context(task_id: str) -> None:
    """End the conductor.task span and detach the context for this Celery task_id, if any."""
    state = _otel_state.pop(task_id, None)
    if state is None:
        return
    if state.span is not None:
        state.span.end()
    if state.token is not None and detach is not None:
        detach(state.token)


def tag_current_span(carrier: dict, conductor_task_id: str, workflow_instance_id: str) -> None:
    """Tag the current span so Datadog can search by correlation and Conductor ids."""
    if trace is None:
        return

    span = trace.get_current_span()
    correlation_id = carrier.get("correlation_id") or carrier.get("traceparent")
    if correlation_id:
        span.set_attribute("correlation_id", correlation_id)
    span.set_attribute("conductor.task_id", conductor_task_id)
    span.set_attribute("conductor.workflow_instance_id", workflow_instance_id)
