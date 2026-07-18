from typing import Type, cast

from airflow_pydantic import ImportPath, Task, TaskArgs
from pydantic import Field, field_validator

from airflow_nomad.config import NomadAirflowConfiguration

__all__ = ("NomadOperator", "NomadOperatorArgs", "NomadTask", "NomadTaskArgs")


class NomadTaskArgs(TaskArgs):
    cfg: NomadAirflowConfiguration


NomadOperatorArgs = NomadTaskArgs


class NomadTask(Task, NomadTaskArgs):
    operator: ImportPath = Field(default=cast(ImportPath, "airflow_nomad.Nomad"), validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, value: Type) -> Type:
        from airflow_nomad.airflow import Nomad

        if value is not Nomad:
            raise ValueError(f"operator must be 'airflow_nomad.Nomad', got: {value}")
        return value


NomadOperator = NomadTask
