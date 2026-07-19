# Why Airflow owns the Nomad job lifecycle

Airflow coordinates a workflow, while Nomad schedules and supervises the
processes that perform one long-running step. `airflow-nomad` keeps those roles
separate: Airflow determines when the work belongs in a DAG run; Nomad chooses
nodes, starts allocations, applies resource limits, and records runtime state.

## Lifecycle mapping

The integration registers a Nomad jobspec and polls the status bundle returned
by `nomad job status -json`. Successful terminal allocations stop monitoring.
Pending or running allocations continue monitoring. Failed or lost current
allocations enter the `airflow-ha` retrigger path.

This mapping uses current job-version allocations rather than Nomad's coarse
job status. Nomad reports both successfully completed and failed batch jobs as
`dead`; the allocation records preserve the distinction Airflow needs.

## Why the integration uses the Nomad CLI

`nomad-pydantic` renders the native JSON jobspec and delegates lifecycle calls
to the Nomad CLI. This keeps address, namespace, TLS, and token handling aligned
with an operator's existing Nomad configuration. It also makes the client easy
to replace with a fake command runner in tests.

Unlike `airflow-systemd`, this integration does not need a separate SSH mode.
The CLI already talks to a remote Nomad API, so workers only need network access
and credentials for the target cluster.

## Comparison with host process managers

[airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor)
creates a dedicated supervisord instance, while
[airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) uses a
host's service manager. `airflow-nomad` targets a cluster scheduler, making it a
better fit when placement, multi-node capacity, resource isolation, and Nomad
service integrations are already operational standards.
