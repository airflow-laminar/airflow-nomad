# How-to guides

Run Nomad jobs from inline Python or `airflow-config` YAML. Both formats use the
Nomad CLI on the Airflow worker to reach the cluster’s API.

## How to select a cluster and namespace

Install the Nomad CLI on every worker that can execute this lifecycle. Set the
connection environment in the worker process, rather than only in an interactive
shell:

```bash
export NOMAD_ADDR=https://nomad.example.com:4646
export NOMAD_NAMESPACE=analytics
export NOMAD_CACERT=/etc/nomad/tls/ca.pem
```

Supply `NOMAD_TOKEN` through your deployment’s secret injection. If the cluster
requires mutual TLS, also supply `NOMAD_CLIENT_CERT` and `NOMAD_CLIENT_KEY`.
The token needs permissions to read, submit, and stop jobs in the target
namespace. See the [Nomad CLI reference](https://developer.hashicorp.com/nomad/commands)
for connection and authentication variables.

Create the namespace before submitting jobs. Have a cluster administrator run:

```bash
nomad namespace apply analytics
```

Set `job.namespace="analytics"` in Python or `namespace: analytics` under `job`
in YAML. This explicit namespace is included in the jobspec and takes precedence
for job-scoped CLI calls. If omitted, the CLI uses `NOMAD_NAMESPACE` or `default`.

Confirm access from the worker account:

```bash
nomad job status -namespace=analytics
```

Select datacenters and drivers available in that cluster. Commands and paths in
a task’s `config` refer to the Nomad client node or task container, rather than
the Airflow worker. The `exec` examples below need Linux Nomad clients with that
driver available. No SSH connection is required for a remote cluster.

## How to run a job in Python

Install `airflow-nomad[airflow]` in Airflow 2, or
`airflow-nomad[airflow3]` in Airflow 3. Complete the
[cluster setup]().

Give all lifecycle tasks the same writable absolute `working_dir`. With several
workers, mount that path on shared storage; configuration, registration, and
cleanup must reach the same jobspec. Every worker also needs the same cluster
credentials. Reserve the job ID for this DAG.

Save this DAG in the DAG folder, adapting the namespace, datacenter, driver,
and command to your cluster:

```python
from datetime import datetime, timezone

from airflow import DAG
from airflow_nomad import Job, Nomad, NomadAirflowConfiguration, Task, TaskGroup

with DAG(
    dag_id="batch-nomad",
    schedule="@daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
) as dag:
    nomad = Nomad(
        dag=dag,
        cfg=NomadAirflowConfiguration(
            working_dir="/var/tmp/batch-nomad",
            job=Job(
                id="airflow-batch",
                type="batch",
                namespace="analytics",
                datacenters=["dc1"],
                task_groups=[
                    TaskGroup(
                        name="batch",
                        tasks=[Task(name="sleep", driver="exec", config={"command": "/bin/sleep", "args": ["5"]})],
                    ),
                ],
            ),
        ),
    )
```

Run `airflow tasks list batch-nomad`, then trigger `batch-nomad`. Registration
starts the job; successful completion stops it and removes the local jobspec.

## How to run a job with airflow-config

Use the packages, cluster, and shared artifact path from the
[Python guide](). Install `airflow-config` in the DAG
parsing environment.

Create `config/batch_nomad.yaml` beside the DAG loader:

```yaml
# @package _global_
_target_: airflow_config.Configuration
_convert_: all

dags:
  batch-nomad:
    schedule: "@daily"
    start_date: "2025-01-01"
    catchup: false
    tasks:
      run:
        _target_: airflow_nomad.NomadTask
        cfg:
          working_dir: /var/tmp/batch-nomad
          job:
            id: airflow-batch
            type: batch
            namespace: analytics
            datacenters: [dc1]
            task_groups:
              - name: batch
                tasks:
                  - name: sleep
                    driver: exec
                    config:
                      command: /bin/sleep
                      args: ["5"]
```

Save `batch_nomad.py` in the DAG folder:

```python
"""Generate Airflow DAGs from the Nomad configuration."""

from airflow_config import load_config

config = load_config("config", "batch_nomad")
config.generate_in_mem()
```

`_convert_: all` converts nested command arguments into Python lists before
model construction, including values inside Nomad’s untyped `config` mapping.

Run `airflow tasks list batch-nomad`, then trigger `batch-nomad`. Deploy either
this loader or the inline Python DAG for that DAG ID. Environment variables
select local or remote clusters in either format.

## How to limit monitoring or retain a service

Set monitoring controls under `cfg` in YAML:

```yaml
check_interval: 00:00:10
check_timeout: 08:00:00
runtime: 04:00:00
maxretrigger: 3
```

In Python, pass `timedelta(seconds=10)`, `timedelta(hours=8)`, and
`timedelta(hours=4)` for the duration fields, importing `timedelta` from
`datetime`. `runtime` or `endtime` ends monitoring through the stop branch;
`check_timeout` is the sensor timeout. See the
[timing reference](api.html.md#monitoring-and-lifecycle-fields) for reference dates.

For a service that should remain running between DAG runs, set
`stop_on_exit=False`, `cleanup=False`, `restart_on_initial=True`, and
`restart_on_retrigger=True` in Python, or under `cfg` in YAML:

```yaml
stop_on_exit: false
cleanup: false
restart_on_initial: true
restart_on_retrigger: true
runtime: 04:00:00
```

Set `job.type="service"` and configure the long-running command or container in
its task group. Keep the job ID, namespace, and artifact paths stable. Set an
end condition for monitoring a service that never exits. Configure Nomad restart
and reschedule policies separately for failures between Airflow runs.

## How to purge job history or clean up a failed run

To purge history when the normal stop task runs, set `purge_on_exit=True` in
Python, or under `cfg` in YAML:

```yaml
stop_on_exit: true
purge_on_exit: true
cleanup: true
```

Stop and cleanup follow successful monitoring. They do not finalize every
failure. After a failed or cancelled run, stop the job from an authorized Nomad
CLI environment, using its actual namespace and ID:

```bash
nomad job stop -namespace=analytics -purge -yes airflow-batch
```

Remove its generated JSON file only after no active run uses it. The generated
force-kill task also requests a purge; its normal parent is skipped, so it is
not an automatic failure cleanup path.

## How to chain the lifecycle with other tasks

In Python, connect existing Airflow tasks to the `Nomad` object:

```python
prepare >> nomad >> publish
```

In `airflow-config`, use the task mapping key in dependencies:

```yaml
tasks:
  prepare:
    _target_: airflow_pydantic.BashTask
    bash_command: echo prepare
  run:
    _target_: airflow_nomad.NomadTask
    _convert_: all
    dependencies: [prepare]
    cfg:
      working_dir: /var/tmp/chained-nomad
      job:
        id: airflow-chained
        type: batch
        namespace: analytics
        datacenters: [dc1]
        task_groups:
          - name: batch
            tasks:
              - name: sleep
                driver: exec
                config:
                  command: /bin/sleep
                  args: ["5"]
  publish:
    _target_: airflow_pydantic.BashTask
    bash_command: echo publish
    dependencies: [run]
```

Upstream tasks precede configuration; downstream tasks follow cleanup. Keep
stop and cleanup enabled for downstream tasks with the default `all_success`
trigger rule. Disabling cleanup creates a skipped boundary task.
