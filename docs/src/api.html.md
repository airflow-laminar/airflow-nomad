# API reference

The public integration API is exported from `airflow_nomad`. Jobspec models and
the CLI client are re-exported from `nomad_pydantic`, whose
[API reference](https://airflow-laminar.github.io/nomad-pydantic/docs/src/api.html)
describes the native job schema.

## Lifecycle class and task models

`Nomad(dag, cfg, **kwargs)` adds lifecycle tasks to an existing Airflow DAG.
`cfg` accepts a `NomadAirflowConfiguration` or a dictionary validated as that
model. The optional `nomad_client` keyword supplies a client.

Other keyword arguments, including `on_failure_callback` and retries, apply to
the generated operators. Lifecycle command failures raise `AirflowException`.
Failed allocation diagnostics include task state, exit code, signal, and events.

`NomadTaskArgs` contains `cfg` and inherited Airflow task arguments.
`NomadTask.operator` defaults to `airflow_nomad.Nomad` and rejects other
operators. `NomadOperator` and `NomadOperatorArgs` alias `NomadTask` and
`NomadTaskArgs`.

### DAG constraints and boundaries

The lifecycle sets `catchup=False`, `concurrency=1`, `max_active_tasks=1`, and
`max_active_runs=1`. Generated task IDs use `<dag_id>-<step>`, independent of a
task model’s `task_id`. A DAG supports one Nomad lifecycle; several workload
tasks belong in its job’s task groups.

The exposed boundaries are `configure_nomad`, `register_job`, `check_job`,
`restart_job`, `stop_job`, and `cleanup_nomad`. Upstream dependencies attach
to configuration; downstream dependencies attach to cleanup. `nomad_client`
returns the client stored by the lifecycle.

Stop and cleanup follow successful monitoring. They are not a failure
finalizer. `cleanup=False` creates a skipped cleanup task. `stop_on_exit=False`
creates skipped stop and cleanup tasks, regardless of `cleanup`. Skipped
boundaries affect downstream `all_success` tasks.

## Configuration model

`NomadAirflowConfiguration` extends `nomad_pydantic.NomadConfiguration`.

### Jobspec and CLI fields

| Field             | Default                          | Meaning                                                                |
|-------------------|----------------------------------|------------------------------------------------------------------------|
| `job`             | Required                         | Native Nomad job model with task groups.                               |
| `working_dir`     | `<cwd>/.nomad-pydantic/<job.id>` | Local jobspec directory; derived on the DAG parsing host when omitted. |
| `path`            | `<working_dir>/<job.id>.json`    | Local generated JSON jobspec path.                                     |
| `command_timeout` | `60`                             | CLI command timeout in seconds; `None` disables it.                    |

`job.id` is required. `job.type` defaults to `service`; accepted values are
`service`, `batch`, `system`, and `sysbatch`. `job.namespace` defaults to `None`.
An explicit namespace is written into the jobspec and passed as `-namespace`
for job-scoped calls. CLI environment variables supply the cluster address,
region, authentication, TLS, and fallback namespace.

`nomad_json()` returns JSON containing only fields accepted by
`NomadConfiguration`, excluding Airflow settings. `write()` writes the native
job envelope accepted by `nomad job run -json`. Cleanup removes `path` and
removes `working_dir` only if empty. It preserves unrelated files and Nomad’s
job history. `purge_on_exit` changes the stop request, separately from local
cleanup.

### Monitoring and lifecycle fields

| Field                  | Default             | Meaning                                                                              |
|------------------------|---------------------|--------------------------------------------------------------------------------------|
| `check_interval`       | 5 seconds           | Polling interval for the `airflow-ha` sensor.                                        |
| `check_timeout`        | 8 hours             | Sensor timeout.                                                                      |
| `runtime`              | `None`              | Monitoring end condition relative to `reference_date`.                               |
| `endtime`              | `None`              | Time-of-day end condition passed to `airflow-ha`.                                    |
| `maxretrigger`         | `None`              | Retrigger limit passed to `airflow-ha`.                                              |
| `reference_date`       | `data_interval_end` | Reference for end conditions; also accepts `start_date` or `logical_date`.           |
| `pool`                 | `None`              | Airflow pool name or `airflow-pydantic` `Pool`.                                      |
| `stop_on_exit`         | `True`              | Enables job stop on the success path.                                                |
| `cleanup`              | `True`              | Enables jobspec removal when `stop_on_exit` is also true.                            |
| `purge_on_exit`        | `False`             | Purges job history during the normal stop step.                                      |
| `restart_on_initial`   | `False`             | Restarts allocations after registration on an initial Airflow run.                   |
| `restart_on_retrigger` | `False`             | Restarts allocations after registration; takes precedence over `restart_on_initial`. |

Duration fields accept Python `timedelta` values or numeric seconds and duration
strings in YAML. They are passed to `HighAvailabilityOperator` as `poke_interval`,
`timeout`, `runtime`, `endtime`, `maxretrigger`, and `reference_date`. The sensor
uses `mode="poke"`.

Registration runs `nomad job run -json -detach`. The restart branch runs
`nomad job restart -yes -all-tasks`. The force-kill task always requests a purge;
its normal parent is a skipped task.

`load_airflow_config` aliases `NomadAirflowConfiguration.load`, which composes
Hydra YAML into a single job configuration. `airflow_config.load_config` loads
collections of declarative DAGs. The [how-to guides](how-to.html.md) cover the latter.

### Log forwarding fields

| Field            | Default   | Meaning                                                         |
|------------------|-----------|-----------------------------------------------------------------|
| `forward_logs`   | `False`   | Forwards allocation task stdout and stderr into Airflow logs.   |
| `log_chunk_size` | `65536`   | Maximum bytes per stream per read; range `1` through `1048576`. |

Forwarding reads allocations from the newest job version. Streams are tagged
with allocation ID, task name, and stream. Disabled Nomad task logs are omitted.
Each status check reads at most one chunk per stream; restart, stop, and purge
drain at most sixteen chunks per stream. Read errors warn without changing the
job’s health result. Split UTF-8 characters can appear as replacement characters;
cursors advance by the original byte count.

`nomad_log_offsets` XComs contain byte offsets indexed by allocation, task,
stream, and log file. Lifecycle tasks share cursors within a DAG run. Scheduled
health tasks read their own prior-run cursors, including failed checks.

## Health callable

`check_nomad_health(cfg, *, nomad_client=None, require_running=True, **context)`
checks an existing job without registering, restarting, or stopping it. `cfg`
accepts a `NomadAirflowConfiguration` or a configuration dictionary.

The default requires a current running allocation. Failed or lost allocations,
failed task states, stopped jobs, and jobs without allocations raise
`AirflowException`. With `require_running=False`, successfully completed batch
jobs can pass. Client status errors propagate to the caller. Enabled forwarding
reads logs before health failure is raised and saves cursors when a task instance
is present.

The returned dictionary contains `job_id`, `namespace`, `running`, `complete`,
and `allocations`, a list of current allocation IDs. Direct calls without a
task instance retain no cursor between calls.

The [observability guide](observability.html.md) covers Python and `airflow-config`
configuration, permissions, alert callbacks, and serialized watchdog runs.

## Generated API

| [`NomadAirflowConfiguration`](_build/airflow_nomad.NomadAirflowConfiguration.html.md#airflow_nomad.NomadAirflowConfiguration)            | Nomad configuration for an Airflow-managed job.                                                    |
|------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| [`NomadTaskArgs`](_build/airflow_nomad.NomadTaskArgs.html.md#airflow_nomad.NomadTaskArgs)                                                |                                                                                                    |
| [`NomadTask`](_build/airflow_nomad.NomadTask.html.md#airflow_nomad.NomadTask)                                                            |                                                                                                    |
| [`NomadOperatorArgs`](_build/airflow_nomad.NomadOperatorArgs.html.md#airflow_nomad.NomadOperatorArgs)                                    | alias of [`NomadTaskArgs`](_build/airflow_nomad.NomadTaskArgs.html.md#airflow_nomad.NomadTaskArgs) |
| [`NomadOperator`](_build/airflow_nomad.NomadOperator.html.md#airflow_nomad.NomadOperator)                                                | alias of [`NomadTask`](_build/airflow_nomad.NomadTask.html.md#airflow_nomad.NomadTask)             |
| [`load_airflow_config`](_build/airflow_nomad.load_airflow_config.html.md#airflow_nomad.load_airflow_config)(config_dir, config_name, \*) | Compose a Hydra YAML configuration and validate it.                                                |
| [`Nomad`](_build/airflow_nomad.Nomad.html.md#airflow_nomad.Nomad)(dag, cfg, \*\*kwargs)                                                  | Airflow task group for a Nomad-managed job.                                                        |
| [`check_nomad_health`](_build/airflow_nomad.check_nomad_health.html.md#airflow_nomad.check_nomad_health)(cfg, \*[, nomad_client, ...])   | Check an existing job once, requiring a running service by default.                                |
