# Tracing

`ConductorTask` strips reserved tracing keys from Conductor `inputData` and restores W3C Trace Context so Datadog / OpenTelemetry keep one trace across the async poll hop. Task function signatures do not take `correlation_id`.

## Design Decisions

### `correlation_id` is a W3C `traceparent`

Conductor is async: the worker polls later and Celery would otherwise start a new root span. Datadog stitches traces with W3C Trace Context (`00-{trace_id}-{parent_span_id}-{flags}`), not with a random UUID.

The workflow starter (outside this library) injects the current context and stores `traceparent` as workflow input `correlation_id`:

```python
from opentelemetry.propagate import inject

carrier = {}
inject(carrier)
workflow_input["correlation_id"] = carrier["traceparent"]
```

Each SIMPLE task must map `${workflow.input.correlation_id}` into `inputData`. Conductor does not forward workflow input automatically.

### Strip in the base class, not in each task

`split_trace_context` copies `input_data` and pops `correlation_id`, `traceparent`, and `tracestate` before `request.kwargs` is set. A W3C-shaped `correlation_id` becomes `carrier["traceparent"]`. An explicit `traceparent` wins if both are present. A non-W3C `correlation_id` is still stripped and kept for span tagging.

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
- `conductor_celery.tracing.tag_current_span(carrier, conductor_task_id, workflow_instance_id)` — sets `correlation_id`, `conductor.task_id`, `conductor.workflow_instance_id`
- Reserved input keys: `correlation_id`, `traceparent`, `tracestate`
- Request headers: `conductor` (task metadata), `otel` (carrier for retries)

`ConductorTask.before_start` polls once, stores headers, then always restores context. `ConductorTask.after_return` calls deactivate.

## Key Learnings / Gotchas

- Workflow JSON and start clients live in the consuming app. This library only strips and re-attaches.
- Every Conductor SIMPLE task that should stay on the trace must map `correlation_id` into its `inputParameters`.
- A business UUID in `correlation_id` is searchable as a span attribute but does not stitch the APM flame graph.
- Tagging the span returned by `extract` alone does nothing (`NonRecordingSpan`). Always start a recording child first.
- `set_tracer_provider` can be called only once; tests share one session-scoped SDK provider in `tests/conftest.py`.
- Workers using ddtrace need `DD_TRACE_OTEL_ENABLED=true`.
