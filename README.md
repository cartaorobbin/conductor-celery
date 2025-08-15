# conductor-celery

A flexible library for integrating Conductor with Celery, with optional extensions for Kafka and Pyramid integration.

[![Release](https://img.shields.io/github/v/release/tomas_correa/conductor-celery)](https://img.shields.io/github/v/release/tomas_correa/conductor-celery)
[![Build status](https://img.shields.io/github/actions/workflow/status/tomas_correa/conductor-celery/main.yml?branch=main)](https://github.com/tomas_correa/conductor-celery/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/tomas_correa/conductor-celery/branch/main/graph/badge.svg)](https://codecov.io/gh/tomas_correa/conductor-celery)
[![Commit activity](https://img.shields.io/github/commit-activity/m/tomas_correa/conductor-celery)](https://img.shields.io/github/commit-activity/m/tomas_correa/conductor-celery)
[![License](https://img.shields.io/github/license/tomas_correa/conductor-celery)](https://img.shields.io/github/license/tomas_correa/conductor-celery)

This library provides a flexible way to use Conductor with Celery, with optional extensions for Kafka and Pyramid integration. The extension system allows you to use the library with or without specific dependencies, making it suitable for various deployment scenarios.

- **Github repository**: <https://github.com/tomas_correa/conductor-celery/>
- **Documentation** <https://tomas_correa.github.io/conductor-celery/>

## Features

- **Core Integration**: Seamless integration between Conductor and Celery
- **Optional Extensions**: Use with or without Kafka and Pyramid
- **Flexible Configuration**: Easy configuration management
- **Context Managers**: Automatic resource management
- **Type Hints**: Full type support for better development experience

## Installation

### For Library Users

#### Basic Installation
```bash
pip install conductor-celery
```

#### With Optional Extensions
```bash
# With Kafka Support
pip install conductor-celery[kafka]

# With Pyramid Support
pip install conductor-celery[pyramid]

# With All Extensions
pip install conductor-celery[all]
```

### For Library Developers

#### Basic Installation
```bash
make install
# or
poetry install
```

#### With Optional Extensions
```bash
# With Kafka Support
make install-kafka
# or
poetry install -E kafka

# With Pyramid Support
make install-pyramid
# or
poetry install -E pyramid

# With All Extensions
make install-all
# or
poetry install -E all
```

> 📖 **See [INSTALLATION.md](INSTALLATION.md) for detailed installation instructions.**

## Quick Start

### Basic Usage
```python
from conductor_celery import tasks, wrapper

# Use the core functionality
task_result = tasks.execute_task("my_task", {"param": "value"})
```

### Using Extensions
```python
from conductor_celery import create_extension, get_manager

# Create Kafka extension
kafka_ext = create_extension("kafka", {
    'bootstrap_servers': ['localhost:9092']
})

# Use with context manager
with kafka_ext:
    kafka_ext.send_message("my-topic", b"Hello, Kafka!")

# Or use the manager
manager = get_manager()
with manager:
    # All extensions are managed automatically
    pass
```

## Getting Started with Development

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