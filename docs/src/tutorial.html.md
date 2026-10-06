# Tutorial: run your first Nomad job

We will run a five-second batch job from Airflow, monitor its completion, and
remove its generated jobspec.

Use an initialized Airflow 2 environment on a Linux development host with the
[Nomad CLI installed](https://developer.hashicorp.com/nomad/docs/deploy).
Run the DAG parser and tasks on that host as the same Unix user. The example
uses `/bin/sleep`, which is already available on Linux.

## Start a development cluster

Create `nomad-dev.hcl` outside your DAG folder:

```text
plugin "raw_exec" {
  config {
    enabled = true
  }
}
```

Start the development agent in another terminal and leave it running:

```bash
sudo nomad agent -dev -config=nomad-dev.hcl
```

This enables an unisolated task driver for the local exercise. Use an isolated
driver and a secured cluster for deployed workloads.

In the Airflow terminal, select the development cluster and check its node:

```bash
export NOMAD_ADDR=http://127.0.0.1:4646
export NOMAD_NAMESPACE=default
nomad node status
```

Wait until one node has status `ready` and scheduling is `eligible`.

## Install the integration

In the Airflow environment, run:

```bash
pip install 'airflow-nomad[airflow]'
```

## Create the DAG

Save this as `nomad_demo.py` in your Airflow DAG folder:

```python
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow_nomad import Job, Nomad, NomadAirflowConfiguration, Task, TaskGroup

with DAG(
    dag_id="nomad-demo",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
) as dag:
    nomad = Nomad(
        dag=dag,
        cfg=NomadAirflowConfiguration(
            working_dir=Path.home() / ".local/state/airflow-nomad-demo",
            job=Job(
                id="airflow-nomad-demo",
                type="batch",
                namespace="default",
                datacenters=["dc1"],
                task_groups=[
                    TaskGroup(
                        name="demo",
                        tasks=[Task(name="sleep", driver="raw_exec", config={"command": "/bin/sleep", "args": ["5"]})],
                    ),
                ],
            ),
        ),
    )
```

List the generated tasks:

```bash
airflow tasks list nomad-demo
```

Look for `nomad-demo-configure-nomad`, `nomad-demo-register-job`, and
`nomad-demo-check-job`. Restart, stop, cleanup, and failure-handling tasks also
appear.

## Run the job

Execute one DAG run locally:

```bash
airflow dags test nomad-demo 2025-01-01
```

Configuration writes the JSON jobspec. Registration submits it to Nomad, which
starts `/bin/sleep` on its development node. After the allocation completes,
monitoring takes its success branch. Stop deregisters the job; cleanup removes
the jobspec. The DAG run finishes with state `success`; restart and force-kill
branches are skipped.

Check the generated directory and retained job record:

```bash
test ! -d "$HOME/.local/state/airflow-nomad-demo" && echo "Jobspec removed"
nomad job status airflow-nomad-demo
```

You should see `Jobspec removed` and a job status of `dead`. Job history remains
until Nomad removes it or you purge it explicitly:

```bash
nomad job stop -purge -yes airflow-nomad-demo
```

Stop the development agent with Ctrl-C in its terminal.

For existing clusters, follow the [Python and YAML guides](how-to.html.md). The
[configuration reference](api.html.md) lists namespace and lifecycle settings.
