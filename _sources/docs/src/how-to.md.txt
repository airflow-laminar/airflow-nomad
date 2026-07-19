# How-to guides

These guides cover common lifecycle and deployment configurations.

## How to keep a service running after the DAG finishes

Disable stop and cleanup, then choose when to restart allocations:

```yaml
cfg:
  stop_on_exit: false
  cleanup: false
  restart_on_initial: true
  restart_on_retrigger: true
  job:
    id: api
    type: service
    task_groups:
      - name: api
        tasks:
          - name: api
            driver: docker
            config:
              image: example/api:latest
```

Use this configuration when Nomad should retain ownership of the service after
the Airflow run ends.

## How to limit monitoring

Set Airflow-specific timing fields on `NomadAirflowConfiguration`:

```yaml
cfg:
  check_interval: 00:00:10
  check_timeout: 08:00:00
  runtime: 04:00:00
  maxretrigger: 3
  job:
    id: report
    type: batch
    task_groups:
      - name: report
        tasks:
          - name: report
            driver: exec
            config:
              command: /opt/jobs/report
```

`check_interval` controls polling, `check_timeout` controls the sensor timeout,
and `runtime` controls the allowed external job duration.

## How to select a Nomad cluster

Set standard Nomad CLI environment variables on every Airflow worker that can
run the lifecycle tasks:

```bash
export NOMAD_ADDR=https://nomad.example.com:4646
export NOMAD_NAMESPACE=analytics
export NOMAD_TOKEN=...
```

Set `job.namespace` in the jobspec when the namespace is part of the declarative
configuration. The explicit job namespace takes precedence for job-scoped CLI
operations.

## How to purge job history

Enable purge when the normal stop task runs:

```yaml
cfg:
  stop_on_exit: true
  purge_on_exit: true
```

The force-kill path always requests a purge.

## How to chain the lifecycle with other tasks

The `Nomad` object behaves like a task group boundary:

```python
prepare >> nomad >> publish
```

Upstream dependencies attach to `configure_nomad`. Downstream dependencies
attach to `cleanup_nomad`, including when cleanup is represented by a skip task.
