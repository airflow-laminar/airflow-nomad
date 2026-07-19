# airflow_nomad.NomadAirflowConfiguration

### *pydantic model* airflow_nomad.NomadAirflowConfiguration

Bases: `NomadConfiguration`

Nomad configuration for an Airflow-managed job.

#### *field* check_interval *: timedelta* *= datetime.timedelta(seconds=5)*

Interval between Nomad status checks

#### *field* check_timeout *: timedelta* *= datetime.timedelta(seconds=28800)*

Timeout for Nomad status checks

#### *field* runtime *: timedelta | None* *= None*

Maximum runtime of the Nomad job

#### *field* endtime *: time | None* *= None*

End time of the Nomad job

#### *field* maxretrigger *: int | None* *= None*

Maximum number of status check retriggers

#### *field* reference_date *: Literal['start_date', 'logical_date', 'data_interval_end']* *= 'data_interval_end'*

Date used to evaluate runtime and endtime

#### *field* pool *: str | Pool | None* *= None*

Airflow pool for Nomad tasks

#### *field* stop_on_exit *: bool* *= True*

Stop the Nomad job when the DAG finishes

#### *field* cleanup *: bool* *= True*

Remove the generated jobspec when the DAG finishes

#### *field* purge_on_exit *: bool* *= False*

Purge Nomad job history when stopping the job

#### *field* restart_on_initial *: bool* *= False*

Restart allocations on an initial Airflow run

#### *field* restart_on_retrigger *: bool* *= False*

Restart allocations when airflow-ha retriggers the job

#### nomad_json() → str

Serialize only fields understood by nomad-pydantic.
