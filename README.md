# conductor-celery

This project is a test!!! Do not use it

[![Release](https://img.shields.io/github/v/release/tomas_correa/conductor-celery)](https://img.shields.io/github/v/release/tomas_correa/conductor-celery)
[![Build status](https://img.shields.io/github/actions/workflow/status/tomas_correa/conductor-celery/main.yml?branch=main)](https://github.com/tomas_correa/conductor-celery/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/tomas_correa/conductor-celery/branch/main/graph/badge.svg)](https://codecov.io/gh/tomas_correa/conductor-celery)
[![Commit activity](https://img.shields.io/github/commit-activity/m/tomas_correa/conductor-celery)](https://img.shields.io/github/commit-activity/m/tomas_correa/conductor-celery)
[![License](https://img.shields.io/github/license/tomas_correa/conductor-celery)](https://img.shields.io/github/license/tomas_correa/conductor-celery)

This is a template repository for Python projects that use Poetry for their dependency management.

- **Github repository**: <https://github.com/tomas_correa/conductor-celery/>
- **Documentation** <https://tomas_correa.github.io/conductor-celery/>

## Tracing through Conductor

Celery polls Conductor later, so the worker would start a new Datadog / OpenTelemetry trace unless you pass the current W3C context.

When starting a workflow, inject the current span and send it as `correlation_id`:

```python
from opentelemetry.propagate import inject

carrier = {}
inject(carrier)
workflow_input["correlation_id"] = carrier["traceparent"]
```

Map that field on every SIMPLE task (`Conductor` does not forward workflow input automatically):

```json
"inputParameters": {
  "correlation_id": "${workflow.input.correlation_id}"
}
```

`ConductorTask` strips `correlation_id` (and optional `traceparent` / `tracestate`) from the task kwargs, restores the parent context, and records a `conductor.task` span. Task functions do not take `correlation_id`.

If the worker uses Datadog `ddtrace`, set `DD_TRACE_OTEL_ENABLED=true` so the attached OpenTelemetry context is visible to Datadog.

## Getting started with your project

First, create a repository on GitHub with the same name as this project, and then run the following commands:

``` bash
git init -b main
git add .
git commit -m "init commit"
git remote add origin git@github.com:tomas_correa/conductor-celery.git
git push -u origin main
```

Finally, install the environment and the pre-commit hooks with 

```bash
make install
```

You are now ready to start development on your project! The CI/CD
pipeline will be triggered when you open a pull request, merge to main,
or when you create a new release.

To finalize the set-up for publishing to PyPi or Artifactory, see
[here](https://fpgmaas.github.io/cookiecutter-poetry/features/publishing/#set-up-for-pypi).
For activating the automatic documentation with MkDocs, see
[here](https://fpgmaas.github.io/cookiecutter-poetry/features/mkdocs/#enabling-the-documentation-on-github).
To enable the code coverage reports, see [here](https://fpgmaas.github.io/cookiecutter-poetry/features/codecov/).

## Releasing a new version

- Create an API Token on [Pypi](https://pypi.org/).
- Add the API Token to your projects secrets with the name `PYPI_TOKEN` by visiting 
[this page](https://github.com/tomas_correa/conductor-celery/settings/secrets/actions/new).
- Create a [new release](https://github.com/tomas_correa/conductor-celery/releases/new) on Github. 
Create a new tag in the form ``*.*.*``.

For more details, see [here](https://fpgmaas.github.io/cookiecutter-poetry/features/cicd/#how-to-trigger-a-release).

---

Repository initiated with [fpgmaas/cookiecutter-poetry](https://github.com/fpgmaas/cookiecutter-poetry).