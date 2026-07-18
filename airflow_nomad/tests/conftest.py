from pathlib import Path

import pytest
from nomad_pydantic import Job, Task, TaskGroup

from airflow_nomad import NomadAirflowConfiguration


@pytest.fixture
def nomad_airflow_configuration(tmp_path: Path) -> NomadAirflowConfiguration:
    return NomadAirflowConfiguration(
        job=Job(
            id="test-job",
            type="batch",
            namespace="analytics",
            task_groups=[TaskGroup(name="job", tasks=[Task(name="job", driver="exec", config={"command": "/bin/true"})])],
        ),
        path=tmp_path / "state" / "test-job.json",
        working_dir=tmp_path / "state",
        pool="nomad-pool",
    )
