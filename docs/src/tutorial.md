# Tutorial: run a Nomad job from Airflow

In this tutorial, we will define a Nomad batch job in `airflow-config` YAML and
load it as an Airflow DAG.

## Install the packages

For Airflow 3, run:

```bash
pip install 'airflow-nomad[airflow3]' airflow-config
```

Use the `airflow` extra when running Airflow 2. Every worker that can execute
the generated tasks must have the Nomad CLI and cluster credentials.

## Define the DAG

Create `config/nomad.yaml`:

```yaml
dags:
  nightly-nomad:
    schedule: "@daily"
    start_date: "2024-01-01"
    catchup: false
    tasks:
      run-nightly:
        _target_: airflow_nomad.NomadTask
        cfg:
          stop_on_exit: true
          cleanup: true
          job:
            id: nightly
            type: batch
            namespace: analytics
            datacenters:
              - dc1
            task_groups:
              - name: nightly
                tasks:
                  - name: nightly
                    driver: exec
                    config:
                      command: /opt/jobs/nightly
                    resources:
                      cpu: 500
                      memory_mb: 256
```

## Load the configuration

Create `nightly_nomad.py` in the DAG folder:

```python
from airflow_config import load_config

config = load_config("config", "nomad")
config.generate_in_mem()
```

## Inspect the lifecycle

Parse the DAG folder:

```bash
airflow dags list | grep nightly-nomad
airflow tasks list nightly-nomad
```

The task list includes configure, register, check, restart, stop, and cleanup
steps created by `Nomad`.

Trigger the DAG in a test environment connected to the Nomad cluster. The check
step remains active while current allocations run and completes after Nomad
reports successful terminal allocations.

You have now connected a declarative Airflow DAG to a Nomad-managed batch job.
