# airflow-nomad

Run and monitor Nomad-managed jobs from Apache Airflow.

[![Build Status](https://github.com/airflow-laminar/airflow-nomad/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-nomad/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-nomad/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-nomad)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-nomad)](https://github.com/airflow-laminar/airflow-nomad)
[![PyPI](https://img.shields.io/pypi/v/airflow-nomad.svg)](https://pypi.python.org/pypi/airflow-nomad)

```python
from airflow import DAG
from airflow_nomad import Job, Nomad, NomadAirflowConfiguration, Task, TaskGroup

dag = DAG(dag_id="nightly-nomad", schedule="@daily")
config = NomadAirflowConfiguration(
    job=Job(
        id="nightly",
        type="batch",
        task_groups=[
            TaskGroup(
                name="nightly",
                tasks=[Task(name="nightly", driver="exec", config={"command": "/opt/jobs/nightly"})],
            )
        ],
    )
)
Nomad(dag=dag, cfg=config)
```

The generated task lifecycle writes and registers the jobspec, monitors current
allocations with `airflow-ha`, handles retriggers, stops the job, and optionally
removes the generated configuration.

## Documentation

- [Tutorial: run a Nomad job from Airflow](docs/src/tutorial.md)
- [How-to guides](docs/src/how-to.md)
- [Why Airflow owns the lifecycle](docs/src/explanation.md)
- [API reference](docs/src/api.md)

Published documentation is available at
[airflow-laminar.github.io/airflow-nomad](https://airflow-laminar.github.io/airflow-nomad/).

## Ecosystem

- [nomad-pydantic](https://github.com/airflow-laminar/nomad-pydantic) supplies jobspec models and the Nomad CLI client.
- [supervisor-pydantic](https://github.com/airflow-laminar/supervisor-pydantic), [systemd-pydantic](https://github.com/airflow-laminar/systemd-pydantic), and [cron-pydantic](https://github.com/airflow-laminar/cron-pydantic) model alternative runtimes.
- [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) and [airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) provide analogous long-running job lifecycles.
- [airflow-cron](https://github.com/airflow-laminar/airflow-cron) converts cron jobs into ordinary Airflow tasks.
- [airflow-pydantic](https://github.com/airflow-laminar/airflow-pydantic) supplies declarative task and connection models.
- [airflow-config](https://github.com/airflow-laminar/airflow-config) produces YAML-driven DAGs.

#### NOTE
This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
