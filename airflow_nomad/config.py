from datetime import time, timedelta
from typing import Literal

from airflow_pydantic import Pool
from nomad_pydantic import NomadConfiguration
from pydantic import Field

__all__ = ("NomadAirflowConfiguration", "load_airflow_config")


class NomadAirflowConfiguration(NomadConfiguration):
    """Nomad configuration for an Airflow-managed job."""

    check_interval: timedelta = Field(default=timedelta(seconds=5), description="Interval between Nomad status checks")
    check_timeout: timedelta = Field(default=timedelta(hours=8), description="Timeout for Nomad status checks")

    runtime: timedelta | None = Field(default=None, description="Maximum runtime of the Nomad job")
    endtime: time | None = Field(default=None, description="End time of the Nomad job")
    maxretrigger: int | None = Field(default=None, description="Maximum number of status check retriggers")
    reference_date: Literal["start_date", "logical_date", "data_interval_end"] = Field(
        default="data_interval_end",
        description="Date used to evaluate runtime and endtime",
    )

    pool: str | Pool | None = Field(default=None, description="Airflow pool for Nomad tasks")
    stop_on_exit: bool = Field(default=True, description="Stop the Nomad job when the DAG finishes")
    cleanup: bool = Field(default=True, description="Remove the generated jobspec when the DAG finishes")
    purge_on_exit: bool = Field(default=False, description="Purge Nomad job history when stopping the job")
    restart_on_initial: bool = Field(default=False, description="Restart allocations on an initial Airflow run")
    restart_on_retrigger: bool = Field(default=False, description="Restart allocations when airflow-ha retriggers the job")

    def nomad_json(self) -> str:
        """Serialize only fields understood by nomad-pydantic."""

        data = self.model_dump(include=set(NomadConfiguration.model_fields))
        return NomadConfiguration.model_validate(data).model_dump_json(exclude_unset=True)


load_airflow_config = NomadAirflowConfiguration.load
