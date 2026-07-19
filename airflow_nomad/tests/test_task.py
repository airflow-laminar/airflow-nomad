import pytest
from pydantic import ValidationError

from airflow_nomad import Nomad, NomadAirflowConfiguration, NomadOperator, NomadOperatorArgs, NomadTask, NomadTaskArgs


def test_task_args(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    args = NomadTaskArgs(cfg=nomad_airflow_configuration)

    assert args.cfg == nomad_airflow_configuration
    assert NomadOperatorArgs is NomadTaskArgs


def test_task_default_operator(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    task = NomadTask(task_id="nomad-task", cfg=nomad_airflow_configuration)

    assert task.operator is Nomad
    assert NomadOperator is NomadTask


def test_task_rejects_another_operator(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    with pytest.raises(ValidationError, match="operator must be"):
        NomadTask(task_id="nomad-task", cfg=nomad_airflow_configuration, operator="airflow_nomad.NomadTask")
