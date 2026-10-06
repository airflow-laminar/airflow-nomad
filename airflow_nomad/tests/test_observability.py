import logging
from unittest.mock import Mock

import pytest
from airflow.exceptions import AirflowException
from airflow_ha import Action, Result
from nomad_pydantic import AllocationStatus, CommandResult, JobStatus, JobSummary, LogChunk

from airflow_nomad import Nomad, NomadAirflowConfiguration, check_nomad_health
from airflow_nomad.airflow import Step
from airflow_nomad.tests.test_airflow import FakeDAG, fake_airflow  # noqa: F401


def status(state: str = "running", version: int = 1) -> JobStatus:
    return JobStatus(
        summary=JobSummary(job_id="test-job", namespace="analytics", summary={"job": {state: 1}}),
        evaluations=[],
        allocations=[
            AllocationStatus(
                id="allocation",
                job_version=version,
                task_group="job",
                desired_status="run",
                client_status=state,
                task_states={
                    "job": {
                        "state": "dead" if state == "failed" else state,
                        "events": [{"type": "Terminated", "exit_code": 7, "display_message": "Exit Code: 7"}],
                    }
                },
            )
        ],
    )


def test_logs_are_incremental_and_include_both_streams(nomad_airflow_configuration: NomadAirflowConfiguration, caplog) -> None:
    caplog.set_level(logging.INFO)
    nomad_airflow_configuration.forward_logs = True
    client = Mock()
    client.status.return_value = status()

    def read_logs(allocation, task, stream, *, offsets, limit):
        filename = f"job.{stream}.0"
        return [] if filename in offsets else [LogChunk(filename, 0, f"{stream} output\n".encode())]

    client.read_logs.side_effect = read_logs
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=client)
    check = nomad.check_job.python_callable
    assert check() == (Result.PASS, Action.CONTINUE)
    assert check() == (Result.PASS, Action.CONTINUE)
    assert caplog.text.count("stdout output") == 1
    assert caplog.text.count("stderr output") == 1
    assert "[nomad allocation/job stderr]" in caplog.text


def test_log_read_failure_does_not_fail_health(nomad_airflow_configuration: NomadAirflowConfiguration, caplog) -> None:
    nomad_airflow_configuration.forward_logs = True
    client = Mock()
    client.status.return_value = status()
    client.read_logs.side_effect = RuntimeError("permission denied")
    assert check_nomad_health(nomad_airflow_configuration, nomad_client=client)["running"]
    assert "permission denied" in caplog.text


def test_health_failure_has_task_exit_diagnostics(nomad_airflow_configuration: NomadAirflowConfiguration, caplog) -> None:
    client = Mock()
    client.status.return_value = status("failed")
    with pytest.raises(AirflowException, match="allocation/job.*exit=7"):
        check_nomad_health(nomad_airflow_configuration, nomad_client=client)
    assert "exit_code=7" in caplog.text
    client.read_logs.assert_not_called()


@pytest.mark.parametrize("step", ["register-job", "restart-job", "stop-job", "force-kill"])
def test_unsuccessful_lifecycle_result_raises(nomad_airflow_configuration: NomadAirflowConfiguration, step: Step) -> None:
    client = Mock()
    client.register.return_value = client.restart.return_value = client.stop.return_value = CommandResult(7, "", "rejected")
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=client)
    with pytest.raises(AirflowException, match="exit 7.*rejected"):
        nomad.get_step_kwargs(step)["python_callable"]()


def test_callbacks_propagate_to_generated_tasks(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    callback = Mock()
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=Mock(), on_failure_callback=callback, retries=2)
    assert all(task.kwargs["on_failure_callback"] is callback for task in nomad._dag.tasks)
    assert all(task.kwargs["retries"] == 2 for task in nomad._dag.tasks)


def test_final_logs_precede_purge(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    nomad_airflow_configuration.forward_logs = True
    calls = []
    client = Mock()
    client.status.return_value = status()
    client.read_logs.side_effect = lambda *args, **kwargs: calls.append("read") or []
    client.stop.side_effect = lambda **kwargs: calls.append("stop") or CommandResult(0, "", "")
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=client)
    nomad.get_step_kwargs("force-kill")["python_callable"]()
    assert calls == ["read", "read", "stop"]


def test_health_requires_running_unless_batch_allowed(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    client = Mock()
    client.status.return_value = status("complete")
    with pytest.raises(AirflowException, match="unhealthy"):
        check_nomad_health(nomad_airflow_configuration, nomad_client=client)
    assert check_nomad_health(nomad_airflow_configuration, nomad_client=client, require_running=False)["complete"]


def test_old_versions_are_not_forwarded(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    nomad_airflow_configuration.forward_logs = True
    client = Mock()
    current = status()
    old = current.allocations[0].model_copy(update={"id": "old", "job_version": 0})
    current.allocations.append(old)
    client.status.return_value = current
    client.read_logs.return_value = []
    check_nomad_health(nomad_airflow_configuration, nomad_client=client)
    assert {call.args[0] for call in client.read_logs.call_args_list} == {"allocation"}


def test_log_cursors_restore_across_task_processes(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    nomad_airflow_configuration.forward_logs = True
    client = Mock()
    client.status.return_value = status()
    client.read_logs.return_value = []
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=client)
    ti = Mock()
    cursor = {"allocation/job/stdout": {"job.stdout.0": 42}}
    ti.xcom_pull.return_value = cursor
    nomad.check_job.python_callable(ti=ti, task=nomad.check_job)
    assert client.read_logs.call_args_list[0].kwargs["offsets"] == {"job.stdout.0": 42}
    ti.xcom_push.assert_called_once_with(key="nomad_log_offsets", value=cursor)


def test_serialized_task_id_does_not_override_step_ids(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    nomad = Nomad(FakeDAG(), nomad_airflow_configuration, nomad_client=Mock(), task_id="wrapper")
    assert "task_id" not in nomad.get_base_operator_kwargs()
    assert nomad.check_job.task_id == "nomad-test-check-job"
