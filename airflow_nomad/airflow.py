from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from airflow_pydantic import Pool, fail, skip
from nomad_pydantic import NomadClient

from airflow_nomad.config import NomadAirflowConfiguration

if TYPE_CHECKING:
    from airflow_ha import CheckResult, HighAvailabilityOperator

    DAG = Any
    Operator = Any

__all__ = ("Nomad",)

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


class Nomad:
    """Airflow task group for a Nomad-managed job."""

    def __init__(self, dag: DAG, cfg: NomadAirflowConfiguration | dict[str, Any], **kwargs: Any):
        if isinstance(cfg, dict):
            cfg = NomadAirflowConfiguration.model_validate(cfg)

        self._cfg = cfg
        self._pool = cfg.pool.pool if isinstance(cfg.pool, Pool) else cfg.pool
        self._nomad_client = kwargs.pop("nomad_client", NomadClient(cfg))
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
            trigger_rule="one_failed",
            **self.get_base_operator_kwargs(),
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
        return {"dag": self._dag, "pool": self._pool}

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
                registered = self.nomad_client.register().returncode == 0
                restart = cfg.restart_on_retrigger or (cfg.restart_on_initial and self.check_job.is_initial_run(**kwargs))
                if registered and restart:
                    return self.nomad_client.restart().returncode == 0
                return registered

            return {"python_callable": _register_job, "do_xcom_push": True}
        if step == "stop-job":
            return {
                "python_callable": lambda: self.nomad_client.stop(purge=cfg.purge_on_exit).returncode == 0,
                "do_xcom_push": True,
            }
        if step == "check-job":

            def _check_job(**kwargs: Any) -> CheckResult:
                from airflow_ha import Action, Result

                status = self.nomad_client.status()
                if status.complete or status.stopped:
                    return Result.PASS, Action.STOP
                if status.failed:
                    return Result.FAIL, Action.RETRIGGER
                return Result.PASS, Action.CONTINUE

            return {"python_callable": _check_job, "do_xcom_push": True}
        if step == "restart-job":
            return {"python_callable": lambda: self.nomad_client.restart().returncode == 0, "do_xcom_push": True}
        if step == "cleanup-nomad":
            return {"python_callable": lambda: _remove_jobspec(cfg.path, cfg.working_dir), "do_xcom_push": True}
        if step == "force-kill":
            return {"python_callable": lambda: self.nomad_client.stop(purge=True).returncode == 0, "do_xcom_push": True}
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
