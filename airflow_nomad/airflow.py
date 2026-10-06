from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from airflow_pydantic import Pool, fail, skip
from nomad_pydantic import NomadClient

from airflow_nomad.config import NomadAirflowConfiguration

if TYPE_CHECKING:
    from airflow_ha import CheckResult, HighAvailabilityOperator
    from nomad_pydantic import CommandResult, JobStatus

    DAG = Any
    Operator = Any

__all__ = ("Nomad", "check_nomad_health")

Step = Literal["check-job", "cleanup-nomad", "configure-nomad", "force-kill", "register-job", "restart-job", "stop-job"]


def _python_operator() -> Any:
    from airflow_pydantic.airflow import PythonOperator

    return PythonOperator


def _remove_jobspec(path: Path | None, working_dir: Path | None) -> bool:
    if path is not None and path.exists():
        path.unlink()
    if working_dir is not None and working_dir.exists():
        try:
            working_dir.rmdir()
        except OSError:
            pass
    return True


def _forward_logs(
    cfg: NomadAirflowConfiguration,
    client: NomadClient,
    status: JobStatus,
    log: Any,
    offsets_by_stream: dict[str, dict[str, int]],
    *,
    final: bool = False,
) -> None:
    groups = {group.name: group for group in cfg.job.task_groups}
    if not status.allocations:
        return
    version = max(allocation.job_version for allocation in status.allocations)
    for allocation in status.allocations:
        if allocation.job_version != version:
            continue
        group = groups.get(allocation.task_group)
        if group is None:
            continue
        for workload in group.tasks:
            if workload.log_config is not None and workload.log_config.disabled:
                continue
            for stream in ("stdout", "stderr"):
                key = f"{allocation.id}/{workload.name}/{stream}"
                offsets = offsets_by_stream.setdefault(key, {})
                try:
                    for _ in range(16 if final else 1):
                        chunks = client.read_logs(allocation.id, workload.name, stream, offsets=offsets, limit=cfg.log_chunk_size)
                        for chunk in chunks:
                            if chunk.truncated:
                                log.warning("Nomad log truncated: allocation=%s task=%s stream=%s", allocation.id, workload.name, stream)
                            for line in chunk.data.decode("utf-8", errors="replace").splitlines():
                                log.info("[nomad %s/%s %s] %s", allocation.id, workload.name, stream, line)
                            offsets[chunk.file] = chunk.offset + len(chunk.data)
                        if sum(len(chunk.data) for chunk in chunks) < cfg.log_chunk_size:
                            break
                    else:
                        log.warning("Nomad final log drain reached byte limit: allocation=%s task=%s stream=%s", allocation.id, workload.name, stream)
                except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
                    log.warning("Cannot read Nomad logs: allocation=%s task=%s stream=%s: %s", allocation.id, workload.name, stream, error)


def _log_failure(status: JobStatus, log: Any) -> None:
    for allocation in status.current_allocations:
        if allocation.client_status not in {"failed", "lost"}:
            continue
        log.error("Nomad allocation failed: id=%s group=%s status=%s", allocation.id, allocation.task_group, allocation.client_status)
        for name, state in getattr(allocation, "task_states", {}).items():
            for event in state.events:
                log.error(
                    "Nomad task event: allocation=%s task=%s state=%s type=%s exit_code=%s signal=%s message=%s",
                    allocation.id,
                    name,
                    state.state,
                    event.type,
                    event.exit_code,
                    event.signal,
                    event.display_message,
                )


class Nomad:
    """Airflow task group for a Nomad-managed job."""

    def __init__(self, dag: DAG, cfg: NomadAirflowConfiguration | dict[str, Any], **kwargs: Any):
        if isinstance(cfg, dict):
            cfg = NomadAirflowConfiguration.model_validate(cfg)

        self._cfg = cfg
        self._pool = cfg.pool.pool if isinstance(cfg.pool, Pool) else cfg.pool
        self._nomad_client = kwargs.pop("nomad_client", NomadClient(cfg))
        kwargs.pop("task_id", None)
        self._operator_kwargs = kwargs
        self._log_offsets: dict[str, dict[str, int]] = {}
        self._dag = dag

        self.setup_dag()
        self.initialize_tasks()

        self.configure_nomad >> self.register_job >> self.check_job
        self.check_job.retrigger_fail >> self.restart_job
        self.check_job.stop_pass >> self.stop_job >> self.cleanup_nomad

        self._force_kill = self.get_step_operator("force-kill")
        PythonOperator = _python_operator()

        (
            PythonOperator(
                task_id=f"{self._dag.dag_id}-force-kill-dag",
                python_callable=skip,
                **self.get_base_operator_kwargs(),
            )
            >> self._force_kill
        )

        any_config_fail = PythonOperator(
            task_id=f"{self._dag.dag_id}-check-config-failed",
            python_callable=fail,
            **{**self.get_base_operator_kwargs(), "trigger_rule": "one_failed"},
        )
        self.configure_nomad >> any_config_fail
        self.register_job >> any_config_fail
        self.stop_job >> any_config_fail
        self.cleanup_nomad >> any_config_fail

    def setup_dag(self) -> None:
        self._dag.catchup = False
        self._dag.concurrency = 1
        self._dag.max_active_tasks = 1
        self._dag.max_active_runs = 1

    def initialize_tasks(self) -> None:
        PythonOperator = _python_operator()

        self._check_job = self.get_step_operator("check-job")
        self._configure_nomad = self.get_step_operator("configure-nomad")
        self._register_job = self.get_step_operator("register-job")
        if self._cfg.stop_on_exit:
            self._stop_job = self.get_step_operator("stop-job")
            if self._cfg.cleanup:
                self._cleanup_nomad = self.get_step_operator("cleanup-nomad")
            else:
                self._cleanup_nomad = PythonOperator(
                    task_id=f"{self._dag.dag_id}-cleanup-nomad",
                    python_callable=skip,
                    **self.get_base_operator_kwargs(),
                )
        else:
            self._stop_job = PythonOperator(
                task_id=f"{self._dag.dag_id}-stop-job",
                python_callable=skip,
                **self.get_base_operator_kwargs(),
            )
            self._cleanup_nomad = PythonOperator(
                task_id=f"{self._dag.dag_id}-cleanup-nomad",
                python_callable=skip,
                **self.get_base_operator_kwargs(),
            )
        self._restart_job = self.get_step_operator("restart-job")

    @property
    def configure_nomad(self) -> Operator:
        return self._configure_nomad

    @property
    def register_job(self) -> Operator:
        return self._register_job

    @property
    def check_job(self) -> HighAvailabilityOperator:
        return self._check_job

    @property
    def stop_job(self) -> Operator:
        return self._stop_job

    @property
    def restart_job(self) -> Operator:
        return self._restart_job

    @property
    def cleanup_nomad(self) -> Operator:
        return self._cleanup_nomad

    @property
    def nomad_client(self) -> NomadClient:
        return self._nomad_client

    def get_base_operator_kwargs(self) -> dict[str, Any]:
        return {**self._operator_kwargs, "dag": self._dag, "pool": self._pool}

    def _forward_logs(self, status: JobStatus, context: dict[str, Any], *, final: bool = False) -> None:
        if not self._cfg.forward_logs:
            return
        task = context.get("task", self.check_job)
        log = getattr(task, "log", logging.getLogger("airflow.task"))
        ti = context.get("ti") or context.get("task_instance")
        if ti is not None:
            saved = ti.xcom_pull(task_ids=task.task_id, key="nomad_log_offsets")
            if saved is None:
                saved = ti.xcom_pull(task_ids=self.check_job.task_id, key="nomad_log_offsets")
            self._log_offsets = saved or {}
        _forward_logs(self._cfg, self.nomad_client, status, log, self._log_offsets, final=final)
        if ti is not None:
            ti.xcom_push(key="nomad_log_offsets", value=self._log_offsets)

    def _command(self, result: CommandResult) -> bool:
        if result.returncode:
            from airflow.exceptions import AirflowException

            raise AirflowException(f"Nomad command failed (exit {result.returncode}): {result.stderr.strip() or result.stdout.strip()}")
        return True

    def _flush_logs(self, context: dict[str, Any]) -> None:
        if self._cfg.forward_logs:
            self._forward_logs(self.nomad_client.status(), context, final=True)

    def get_step_kwargs(self, step: Step) -> dict[str, Any]:
        cfg = self._cfg
        if step == "configure-nomad":
            return {
                "python_callable": lambda **kwargs: self.check_job.check_end_conditions(**kwargs) is None and str(cfg.write()),
                "do_xcom_push": True,
            }
        if step == "register-job":

            def _register_job(**kwargs: Any) -> bool:
                if self.check_job.check_end_conditions(**kwargs) is not None:
                    return False
                registered = self._command(self.nomad_client.register())
                restart = cfg.restart_on_retrigger or (cfg.restart_on_initial and self.check_job.is_initial_run(**kwargs))
                if registered and restart:
                    self._flush_logs(kwargs)
                    return self._command(self.nomad_client.restart())
                return registered

            return {"python_callable": _register_job, "do_xcom_push": True}
        if step == "stop-job":

            def _stop_job(**kwargs: Any) -> bool:
                self._flush_logs(kwargs)
                return self._command(self.nomad_client.stop(purge=cfg.purge_on_exit))

            return {
                "python_callable": _stop_job,
                "do_xcom_push": True,
            }
        if step == "check-job":

            def _check_job(**kwargs: Any) -> CheckResult:
                from airflow_ha import Action, Result

                status = self.nomad_client.status()
                self._forward_logs(status, kwargs, final=status.complete or status.stopped or status.failed)
                if status.complete or status.stopped:
                    return Result.PASS, Action.STOP
                if status.failed:
                    _log_failure(status, getattr(kwargs.get("task", self.check_job), "log", logging.getLogger("airflow.task")))
                    return Result.FAIL, Action.RETRIGGER
                return Result.PASS, Action.CONTINUE

            return {"python_callable": _check_job, "do_xcom_push": True}
        if step == "restart-job":

            def _restart_job(**kwargs: Any) -> bool:
                self._flush_logs(kwargs)
                return self._command(self.nomad_client.restart())

            return {"python_callable": _restart_job, "do_xcom_push": True}
        if step == "cleanup-nomad":
            return {"python_callable": lambda: _remove_jobspec(cfg.path, cfg.working_dir), "do_xcom_push": True}
        if step == "force-kill":

            def _force_kill(**kwargs: Any) -> bool:
                self._flush_logs(kwargs)
                return self._command(self.nomad_client.stop(purge=True))

            return {"python_callable": _force_kill, "do_xcom_push": True}
        raise NotImplementedError(f"Unknown step: {step}")

    def get_step_operator(self, step: Step) -> Operator:
        from airflow_ha import HighAvailabilityOperator

        PythonOperator = _python_operator()

        if step == "check-job":
            return HighAvailabilityOperator(
                task_id=f"{self._dag.dag_id}-{step}",
                poke_interval=self._cfg.check_interval.total_seconds(),
                timeout=self._cfg.check_timeout.total_seconds(),
                mode="poke",
                runtime=self._cfg.runtime,
                endtime=self._cfg.endtime,
                maxretrigger=self._cfg.maxretrigger,
                reference_date=self._cfg.reference_date,
                **self.get_base_operator_kwargs(),
                **self.get_step_kwargs(step),
            )
        return PythonOperator(
            task_id=f"{self._dag.dag_id}-{step}",
            **self.get_base_operator_kwargs(),
            **self.get_step_kwargs(step),
        )

    def __lshift__(self, other: Operator) -> Operator:
        self.configure_nomad << other
        return self.cleanup_nomad

    def __rshift__(self, other: Operator) -> Operator:
        self.cleanup_nomad >> other
        return other

    def set_upstream(self, other: Operator) -> None:
        self.configure_nomad.set_upstream(other)

    def set_downstream(self, other: Operator) -> None:
        self.cleanup_nomad.set_downstream(other)

    def update_relative(self, other: Operator, upstream: bool = True, edge_modifier: Any = None) -> Nomad:
        if upstream:
            self.configure_nomad.update_relative(other, upstream=True, edge_modifier=edge_modifier)
        else:
            self.cleanup_nomad.update_relative(other, upstream=False, edge_modifier=edge_modifier)
        return self

    @property
    def leaves(self) -> Any:
        return self.cleanup_nomad.leaves

    @property
    def roots(self) -> Any:
        return self.configure_nomad.roots


def check_nomad_health(
    cfg: NomadAirflowConfiguration | dict[str, Any], *, nomad_client: NomadClient | None = None, require_running: bool = True, **context: Any
) -> dict[str, Any]:
    """Check an existing job once, requiring a running service by default.

    Use this callable in a scheduled PythonOperator to monitor persistent jobs.
    Each invocation reads a bounded snapshot of retained logs when enabled.
    """
    from airflow.exceptions import AirflowException

    if isinstance(cfg, dict):
        cfg = NomadAirflowConfiguration.model_validate(cfg)
    client = nomad_client or NomadClient(cfg)
    status = client.status()
    log = getattr(context.get("task"), "log", logging.getLogger("airflow.task"))
    if cfg.forward_logs:
        _forward_logs(cfg, client, status, log, {}, final=True)
    _log_failure(status, log)
    failed_allocations = any(
        allocation.client_status in {"failed", "lost"} or any(state.failed for state in allocation.task_states.values())
        for allocation in status.current_allocations
    )
    if (
        status.failed
        or failed_allocations
        or status.stopped
        or not status.allocations
        or (require_running and not any(allocation.client_status == "running" for allocation in status.current_allocations))
    ):
        details = [
            f"{allocation.id}/{name}: {state.state}; "
            + "; ".join(f"{event.type} exit={event.exit_code} signal={event.signal} {event.display_message}" for event in state.events)
            for allocation in status.current_allocations
            for name, state in allocation.task_states.items()
        ]
        raise AirflowException(
            f"Nomad job {status.id} is unhealthy: "
            + (
                "; ".join(details)
                or f"running={status.running} complete={status.complete} failed={status.failed} stopped={status.stopped} allocations={len(status.allocations)}"
            )
        )
    return {
        "job_id": status.id,
        "namespace": status.namespace,
        "running": status.running,
        "complete": status.complete,
        "allocations": [allocation.id for allocation in status.current_allocations],
    }
