# airflow_nomad.check_nomad_health

### airflow_nomad.check_nomad_health(cfg: [NomadAirflowConfiguration](airflow_nomad.NomadAirflowConfiguration.html.md#airflow_nomad.NomadAirflowConfiguration) | dict[str, Any], , nomad_client: NomadClient | None = None, require_running: bool = True, \*\*context: Any) → dict[str, Any][[source]](../../../_modules/airflow_nomad/airflow.html.md#check_nomad_health)

Check an existing job once, requiring a running service by default.

Use this callable in a scheduled PythonOperator to monitor persistent jobs.
Airflow task XComs retain byte cursors across scheduled health checks.
