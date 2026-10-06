# How to forward Nomad logs and alert on failures

Enable forwarding on the configuration passed to an existing `Nomad` task
group:

```python
cfg.forward_logs = True
cfg.log_chunk_size = 65536
```

For `airflow-config` YAML, set the same fields inside the task's `cfg` mapping:

```yaml
forward_logs: true
log_chunk_size: 65536
```

Grant `read-fs` capability in the job's namespace to the Nomad CLI token on
Airflow workers. Keep Nomad task logging enabled. Open the `check-job` task
log to see stdout and stderr tagged with allocation ID, task name, and stream.
Logs are also drained before restart, stop, and purge; check those task logs
for final output. Each terminal drain reads at most sixteen chunks per stream.
Permission or allocation-file read errors produce warnings and leave job
health checks active.

Log chunks can split lines. A UTF-8 character split across chunk boundaries
can appear as replacement characters in forwarded text; byte cursors still
advance by the original byte count. Keep the default chunk size unless you
need a smaller read bound.

Attach your existing alert callback when constructing the group:

```python
from airflow_nomad import Nomad

Nomad(dag=dag, cfg=cfg, on_failure_callback=report_failed)
```

For YAML, use an importable function path on the task model:

```yaml
on_failure_callback: alerts.report_failed
```

Failed lifecycle commands raise task exceptions. Job failure diagnostics
include allocation ID, task state, exit code, signal, and Nomad task events.
Airflow invokes failure callbacks when tasks exhaust retries. Configure
`maxretrigger` to bound workload recovery attempts; Nomad task restart and
reschedule policies apply independently.

## How to monitor a persistent job between runs

Create a separate scheduled health DAG for a job that continues after its
management DAG finishes. Keep `stop_on_exit=False` on the management
configuration, then pass that configuration to `check_nomad_health`:

```python
from airflow_pydantic import Dag, PythonTask
from airflow_nomad import check_nomad_health

watchdog = Dag(
    dag_id="nomad-health",
    schedule="*/5 * * * *",
    start_date="2025-01-01",
    catchup=False,
    tasks={
        "health": PythonTask(
            python_callable=check_nomad_health,
            op_kwargs={"cfg": cfg.model_dump()},
            on_failure_callback=report_failed,
            retries=0,
        ),
    },
)
watchdog.instantiate()
```

For `airflow-config`, save `config/nomad_health.yaml` beside your DAG loader.
Supply the job configuration as `op_kwargs.cfg`:

```yaml
# @package _global_
_target_: airflow_config.Configuration
_convert_: all

dags:
  nomad-health:
    schedule: "*/5 * * * *"
    start_date: "2025-01-01"
    catchup: false
    tasks:
      health:
        _target_: airflow_pydantic.PythonTask
        python_callable: airflow_nomad.check_nomad_health
        on_failure_callback: alerts.report_failed
        retries: 0
        op_kwargs:
          cfg:
            forward_logs: true
            job:
              id: worker
              namespace: default
              type: service
              task_groups:
                - name: worker
                  tasks:
                    - name: worker
                      driver: exec
                      config:
                        command: /opt/jobs/worker
```

Save `nomad_health.py` in your DAG folder:

```python
"""Generate Airflow DAGs for Nomad health checks."""

from airflow_config import load_config

load_config("config", "nomad_health").generate_in_mem()
```

The health callable checks the existing job without registering, restarting, or stopping
it. A missing, stopped, failed, or completed persistent job fails the health
task. Set `op_kwargs.require_running: false` if successfully completed batch
jobs should pass. Watchdog tasks keep byte cursors in `nomad_log_offsets`
XComs and read them from prior runs, including failed checks, to avoid replaying
retained output. Keep XCom history for this health task to preserve those
cursors. Direct calls without an Airflow task instance read a bounded snapshot
of retained logs on each invocation. A crash and
recovery entirely between checks may be missed; shorten the schedule to match
your detection requirement.
