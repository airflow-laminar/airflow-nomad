# Why Airflow owns the Nomad job lifecycle

Airflow coordinates workflows; Nomad schedules and supervises the processes
that perform one step. `airflow-nomad` submits jobs within an Airflow DAG run
and monitors the allocations Nomad creates. Nomad retains responsibility for
placement, resource isolation, and task execution on its client nodes.

## Why allocations determine completion

Nomad reports both successfully completed and failed batch jobs as `dead`.
The job’s coarse status therefore cannot tell Airflow whether the work succeeded.
The integration uses the allocation records from `nomad job status -json`,
filtering to the newest allocation job version and allocations still desired
as `run`.

Successful terminal allocations stop monitoring. Queued, starting, or running
work continues. Failed or lost current allocations take the `airflow-ha`
retrigger path once no work is still running. Older allocations remain useful
as history, but do not determine completion of the current job version.

## Why a remote cluster does not need SSH

`nomad-pydantic` renders a native JSON jobspec and calls the installed Nomad
CLI. That CLI already talks to a remote HTTP API. Airflow workers need the
CLI, network access, and cluster credentials; they do not need a shell on a
Nomad server or client node.

The generated jobspec belongs to the Airflow worker filesystem. The scheduled
command belongs to the Nomad allocation’s environment. Shared artifact storage
allows several Airflow workers to reach the same jobspec while Nomad independently
chooses where to execute the workload. Credentials and explicit namespaces keep
successive lifecycle tasks directed at the same cluster and job.

## Why finite jobs and persistent services need different limits

A batch job can finish and give Airflow a terminal allocation state. A service
usually continues running, so its Airflow monitoring window needs a runtime or
end-time condition. The service can remain active between runs when stop and
cleanup are disabled.

Nomad’s restart and reschedule policies can recover tasks between Airflow
checks and DAG runs. Airflow retriggers coordinate failed work with the larger
workflow. These policies apply at different levels: Nomad can recover an
allocation without creating a new Airflow run, while Airflow can restart the
registered job as part of its retrigger path.

Airflow’s schedule determines when to register and monitor the job. Adding a
Nomad periodic schedule gives Nomad separate control over child job creation.
The current monitor follows the configured job ID, rather than aggregating
periodic child jobs, so directly registered finite jobs provide a clearer
completion boundary for an Airflow run.

## Why cleanup has two owners

Airflow cleanup removes the local JSON jobspec and removes its directory if
empty. Nomad owns job history and allocation data separately. A normal stop
deregisters the job without purging its history; `purge_on_exit` requests a purge.
Logs and allocation files remain subject to Nomad’s retention and garbage
collection.

Stop and cleanup are on the successful monitoring path. A failed registration,
failed monitoring task, or cancelled run can leave artifacts or a running job.
Explicit operational cleanup is therefore separate from ordinary successful
completion. Downstream dependencies use the cleanup task as their boundary;
disabling it also changes the task state downstream operators observe.

## Python and YAML describe the same lifecycle

Inline Python constructs `Nomad` with a typed configuration beside other Airflow
code. `airflow-config` loads a `NomadTask` from YAML and constructs the same
lifecycle class. Hydra composition changes how configuration is assembled,
while cluster selection and allocation monitoring remain the same.

Both forms generate task IDs from the DAG ID and allow one Nomad lifecycle per
DAG. Several Nomad tasks or task groups belong within that lifecycle’s job. This
matches the integration’s single active run and single-task concurrency limits.

## Nomad and host process managers

[airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor)
creates a dedicated supervisord instance.
[airflow-systemd](https://github.com/airflow-laminar/airflow-systemd) manages units
on a host’s existing service manager. Nomad adds cluster placement and allocation
history. It fits workflows whose runtime already depends on multiple client
nodes, resource scheduling, and Nomad deployment policies.
