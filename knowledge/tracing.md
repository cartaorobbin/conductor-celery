# Tracing

`ConductorTask` strips reserved tracing keys from Conductor `inputData` and restores W3C Trace Context so Datadog / OpenTelemetry keep one trace across the async poll hop. Task function signatures do not take `traceparent`.

## Design Decisions

### Workflow input is `traceparent`

Conductor is async: the worker polls later and Celery would otherwise start a new root span. Datadog stitches traces with W3C Trace Context (`00-{trace_id}-{parent_span_id}-{flags}`). The same name is used everywhere: workflow input, task `inputData`, and the OTel carrier.

The workflow starter (outside this library) injects the current context into workflow input:

```python
from opentelemetry.propagate import inject

carrier = {}
inject(carrier)
workflow_input["traceparent"] = carrier["traceparent"]
```

Each SIMPLE task must map `${workflow.input.traceparent}` into `inputData`. Conductor does not forward workflow input automatically. Optional `tracestate` uses the same W3C name if present.

### Strip in the base class, not in each task

`split_trace_context` copies `input_data` and pops `traceparent` and `tracestate` before `request.kwargs` is set.

### Recording `conductor.task` span after extract

`activate_trace_context` extracts the parent, starts a recording `conductor.task` child span, and attaches that span as current. Tagging after attach writes to this span, not to the remote `NonRecordingSpan` from `extract`. `deactivate_trace_context` ends the span and detaches. State is stored by Celery `task_id`.

### Re-attach on every `before_start`, including retries

The carrier is stored in `request.headers["otel"]` on the first poll. `_restore_trace_context` runs on every `before_start` so a Celery retry (headers already have `conductor`, no second poll) re-attaches after `after_return` detached the previous attempt.

If `opentelemetry-api` is missing, keys are still stripped and attach is a no-op.

`ddtrace` is not a library dependency. Workers that use Datadog's Python tracer need `DD_TRACE_OTEL_ENABLED=true` so `opentelemetry.context.attach` is visible to ddtrace spans.

## API Surface

- `conductor_celery.tracing.split_trace_context(input_data) -> (task_kwargs, carrier)`
- `conductor_celery.tracing.activate_trace_context(task_id, carrier)` — extract parent, start `conductor.task`, attach
- `conductor_celery.tracing.deactivate_trace_context(task_id)` — end span, detach
- `conductor_celery.tracing.tag_current_span(carrier, conductor_task_id, workflow_instance_id)` — sets `traceparent`, `conductor.task_id`, `conductor.workflow_instance_id`
- Reserved input keys: `traceparent`, `tracestate`
- Request headers: `conductor` (task metadata), `otel` (carrier for retries)

`ConductorTask.before_start` polls once, stores headers, then always restores context. `ConductorTask.after_return` calls deactivate.

## Key Learnings / Gotchas

- Workflow JSON and start clients live in the consuming app. This library only strips and re-attaches.
- Every Conductor SIMPLE task that should stay on the trace must map `traceparent` into its `inputParameters`.
- Tagging the span returned by `extract` alone does nothing (`NonRecordingSpan`). Always start a recording child first.
- `set_tracer_provider` can be called only once; tests share one session-scoped SDK provider in `tests/conftest.py`.
- Workers using ddtrace need `DD_TRACE_OTEL_ENABLED=true`.
