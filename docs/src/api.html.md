# API reference

The public integration API is re-exported from `airflow_nomad`. Jobspec models
are re-exported from `nomad_pydantic` and documented in its
[API reference](https://airflow-laminar.github.io/nomad-pydantic/docs/src/api.html).

## Airflow configuration and tasks

| [`NomadAirflowConfiguration`](_build/airflow_nomad.NomadAirflowConfiguration.md#airflow_nomad.NomadAirflowConfiguration)            | Nomad configuration for an Airflow-managed job.                                               |
|-------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| [`NomadTaskArgs`](_build/airflow_nomad.NomadTaskArgs.md#airflow_nomad.NomadTaskArgs)                                                |                                                                                               |
| [`NomadTask`](_build/airflow_nomad.NomadTask.md#airflow_nomad.NomadTask)                                                            |                                                                                               |
| [`NomadOperatorArgs`](_build/airflow_nomad.NomadOperatorArgs.md#airflow_nomad.NomadOperatorArgs)                                    | alias of [`NomadTaskArgs`](_build/airflow_nomad.NomadTaskArgs.md#airflow_nomad.NomadTaskArgs) |
| [`NomadOperator`](_build/airflow_nomad.NomadOperator.md#airflow_nomad.NomadOperator)                                                | alias of [`NomadTask`](_build/airflow_nomad.NomadTask.md#airflow_nomad.NomadTask)             |
| [`load_airflow_config`](_build/airflow_nomad.load_airflow_config.md#airflow_nomad.load_airflow_config)(config_dir, config_name, \*) | Compose a Hydra YAML configuration and validate it.                                           |

## Runtime lifecycle

| [`Nomad`](_build/airflow_nomad.Nomad.md#airflow_nomad.Nomad)(dag, cfg, \*\*kwargs)   | Airflow task group for a Nomad-managed job.   |
|--------------------------------------------------------------------------------------|-----------------------------------------------|
