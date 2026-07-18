from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import airflow_ha
import pytest
from airflow_ha import Action, Result
from nomad_pydantic import CommandResult

import airflow_nomad.airflow as airflow_module
from airflow_nomad import Nomad, NomadAirflowConfiguration

_ORIGINAL_PYTHON_OPERATOR = airflow_module._python_operator


class FakeDAG:
    def __init__(self, dag_id: str = "nomad-test") -> None:
        self.dag_id = dag_id
        self.tasks: list[FakeOperator] = []
        self.catchup = True
        self.concurrency = 0
        self.max_active_tasks = 0
        self.max_active_runs = 0


class FakeOperator:
    def __init__(self, task_id: str, dag: FakeDAG, **kwargs: Any) -> None:
        self.task_id = task_id
        self.dag = dag
        self.kwargs = kwargs
        self.python_callable = kwargs.get("python_callable")
        self.upstream: list[FakeOperator] = []
        self.downstream: list[FakeOperator] = []
        self.leaves = [self]
        self.roots = [self]
        dag.tasks.append(self)

    def __rshift__(self, other: FakeOperator) -> FakeOperator:
        self.downstream.append(other)
        other.upstream.append(self)
        return other

    def __lshift__(self, other: FakeOperator) -> FakeOperator:
        other >> self
        return other

    def set_upstream(self, other: FakeOperator) -> None:
        other >> self

    def set_downstream(self, other: FakeOperator) -> None:
        self >> other

    def update_relative(self, other: FakeOperator, upstream: bool, edge_modifier: Any = None) -> None:
        if upstream:
            self.set_upstream(other)
        else:
            self.set_downstream(other)


class FakeHighAvailabilityOperator(FakeOperator):
    end_condition: object | None = None
    initial_run = False

    @property
    def retrigger_fail(self) -> FakeHighAvailabilityOperator:
        return self

    @property
    def stop_pass(self) -> FakeHighAvailabilityOperator:
        return self

    def check_end_conditions(self, **kwargs: Any) -> object | None:
        return self.end_condition

    def is_initial_run(self, **kwargs: Any) -> bool:
        return self.initial_run


@pytest.fixture(autouse=True)
def fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(airflow_module, "_python_operator", lambda: FakeOperator)
    monkeypatch.setattr(airflow_ha, "HighAvailabilityOperator", FakeHighAvailabilityOperator)


@pytest.fixture
def client() -> Mock:
    value = Mock()
    value.register.return_value = CommandResult(0, "", "")
    value.restart.return_value = CommandResult(0, "", "")
    value.stop.return_value = CommandResult(0, "", "")
    return value


def create_nomad(configuration: NomadAirflowConfiguration | dict[str, Any], client: Mock) -> Nomad:
    return Nomad(dag=FakeDAG(), cfg=configuration, nomad_client=client)


def test_task_graph(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration.model_dump(), client)

    assert nomad.nomad_client is client
    assert nomad.configure_nomad in nomad._dag.tasks
    assert nomad.register_job in nomad._dag.tasks
    assert nomad.check_job in nomad._dag.tasks
    assert nomad.restart_job in nomad._dag.tasks
    assert nomad.stop_job in nomad._dag.tasks
    assert nomad.cleanup_nomad in nomad._dag.tasks
    assert nomad._dag.catchup is False
    assert nomad._dag.max_active_tasks == 1
    assert nomad._dag.max_active_runs == 1


@pytest.mark.parametrize(("stop_on_exit", "cleanup"), [(False, False), (True, False)])
def test_optional_cleanup(
    nomad_airflow_configuration: NomadAirflowConfiguration,
    client: Mock,
    stop_on_exit: bool,
    cleanup: bool,
) -> None:
    cfg = nomad_airflow_configuration.model_copy(update={"stop_on_exit": stop_on_exit, "cleanup": cleanup})
    nomad = create_nomad(cfg, client)

    assert nomad.stop_job is not None
    assert nomad.cleanup_nomad is not None


def test_lifecycle_callbacks(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration, client)
    nomad.check_job.initial_run = True
    nomad._cfg.restart_on_initial = True
    nomad._cfg.purge_on_exit = True

    configured = nomad.get_step_kwargs("configure-nomad")["python_callable"]()
    assert configured == str(nomad_airflow_configuration.path)
    assert nomad.get_step_kwargs("register-job")["python_callable"]() is True
    assert nomad.get_step_kwargs("restart-job")["python_callable"]() is True
    assert nomad.get_step_kwargs("stop-job")["python_callable"]() is True
    assert nomad.get_step_kwargs("force-kill")["python_callable"]() is True
    assert nomad.get_step_kwargs("cleanup-nomad")["python_callable"]() is True

    assert client.register.call_count == 1
    assert client.restart.call_count == 2
    client.stop.assert_any_call(purge=True)
    assert nomad_airflow_configuration.path is not None
    assert not nomad_airflow_configuration.path.exists()

    nomad.check_job.end_condition = "done"
    assert nomad.get_step_kwargs("configure-nomad")["python_callable"]() is False
    assert nomad.get_step_kwargs("register-job")["python_callable"]() is False


def test_register_without_restart(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration, client)

    assert nomad.get_step_kwargs("register-job")["python_callable"]() is True
    client.restart.assert_not_called()


def test_status_mapping(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration, client)
    check = nomad.get_step_kwargs("check-job")["python_callable"]

    client.status.side_effect = [
        SimpleNamespace(complete=True, stopped=False, failed=False),
        SimpleNamespace(complete=False, stopped=True, failed=False),
        SimpleNamespace(complete=False, stopped=False, failed=True),
        SimpleNamespace(complete=False, stopped=False, failed=False),
    ]

    assert check() == (Result.PASS, Action.STOP)
    assert check() == (Result.PASS, Action.STOP)
    assert check() == (Result.FAIL, Action.RETRIGGER)
    assert check() == (Result.PASS, Action.CONTINUE)


def test_cleanup_preserves_nonempty_directory(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad_airflow_configuration.write()
    assert nomad_airflow_configuration.working_dir is not None
    extra = nomad_airflow_configuration.working_dir / "keep.txt"
    extra.write_text("keep")
    nomad = create_nomad(nomad_airflow_configuration, client)

    assert nomad.get_step_kwargs("cleanup-nomad")["python_callable"]() is True
    assert extra.exists()


def test_unknown_step(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration, client)

    with pytest.raises(NotImplementedError, match="Unknown step"):
        nomad.get_step_kwargs("unknown")  # ty: ignore[invalid-argument-type]


def test_remove_jobspec_accepts_unset_paths() -> None:
    assert airflow_module._remove_jobspec(None, None) is True


def test_python_operator_loader() -> None:
    from airflow_pydantic.airflow import PythonOperator

    assert _ORIGINAL_PYTHON_OPERATOR() is PythonOperator


def test_chaining_helpers(nomad_airflow_configuration: NomadAirflowConfiguration, client: Mock) -> None:
    nomad = create_nomad(nomad_airflow_configuration, client)
    before = FakeOperator(task_id="before", dag=nomad._dag)
    after = FakeOperator(task_id="after", dag=nomad._dag)

    assert nomad << before is nomad.cleanup_nomad
    assert nomad >> after is after
    nomad.set_upstream(before)
    nomad.set_downstream(after)
    assert nomad.update_relative(before) is nomad
    assert nomad.update_relative(after, upstream=False) is nomad
    assert nomad.roots
    assert nomad.leaves
