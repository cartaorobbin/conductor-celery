# Architecture

conductor-celery is a Python library that bridges Netflix Conductor workflow tasks with Celery workers. A Celery task subclass polls Conductor for work, runs the worker function with Conductor's input, and writes COMPLETED or FAILED back to Conductor.

## Package Structure

```
conductor_celery/
├── tasks.py       # ConductorTask Celery base class: poll in before_start, update on success/failure
├── utils.py       # configure_runner cache and TaskResult builder
└── wrapper.py     # Worker + TaskRunner subclasses that expose conductor-python private poll/update
tests/
├── conftest.py              # celery_config, celery_includes, task_poll_response fixture
├── test_conductor_task.py   # ConductorTask poll / execute / complete / fail paths
└── test_update_task.py      # shared update_task helper
```

Poetry owns dependencies (`pyproject.toml` + `poetry.lock`). Quality commands live in the Makefile (`make check`, `make test`). GitHub Actions runs quality + tox on push/PR; `on-release-main.yml` publishes to PyPI when a GitHub Release is published.

## Core Design Decisions

### Celery Task subclass as the integration point

`ConductorTask` hooks Celery's `before_start`, `on_success`, `on_failure`, and `__call__`. Polling happens in `before_start` only when the request has no `conductor` header. The poll result replaces `request.kwargs` with Conductor `input_data` and clears `request.args`. Success and failure map to Conductor statuses COMPLETED and FAILED.

### TaskRunner wraps conductor-python private methods

`wrapper.TaskRunner` subclasses the Conductor client's `TaskRunner` and exposes `__poll_task` / `__update_task`. `utils.configure_runner` caches one `TaskRunner` per task name in a module-level `workers` dict.

### Conductor input wins over the Celery call

`ConductorTask.__call__` ignores the caller's args/kwargs when a conductor header is present and runs `self.run(**self.request.kwargs)` instead. Tests that use `task.apply([...])` still observe Conductor poll input, not the apply arguments.

## Component Relationships

```
Conductor server
    ^
    | poll_task / update_task
TaskRunner (wrapper)  <-- configure_runner (utils, cached by task name)
    ^
ConductorTask lifecycle (before_start -> run -> on_success / on_failure)
    ^
Celery worker
```

`update_task` in `utils.py` builds a `TaskResult` (task id, workflow instance, worker id, output map, status). The shared Celery task `update_task` in `tasks.py` is a thinner helper that always marks COMPLETED.

## Key Learnings / Gotchas

- Celery app config must include `conductor_server_api_url`.
- `configure_runner` caches by task name; changing server URL for an already-seen name will reuse the first runner.
- Calling a `ConductorTask` like a function does nothing useful unless `request.headers` already has `conductor`.
- pytest uses `pytest-celery` plus `responses` to stub Conductor HTTP (`/tasks/poll/...` and `/tasks`).
