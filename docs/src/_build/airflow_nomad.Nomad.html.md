# airflow_nomad.Nomad

### *class* airflow_nomad.Nomad(dag: DAG, cfg: [NomadAirflowConfiguration](airflow_nomad.NomadAirflowConfiguration.md#airflow_nomad.NomadAirflowConfiguration) | dict[str, Any], \*\*kwargs: Any)

Bases: `object`

Airflow task group for a Nomad-managed job.

#### \_\_init_\_(dag: DAG, cfg: [NomadAirflowConfiguration](airflow_nomad.NomadAirflowConfiguration.md#airflow_nomad.NomadAirflowConfiguration) | dict[str, Any], \*\*kwargs: Any)

### Methods

| [`__init__`](#airflow_nomad.Nomad.__init__)(dag, cfg, \*\*kwargs)   |    |
|---------------------------------------------------------------------|----|
| `get_base_operator_kwargs`()                                        |    |
| `get_step_kwargs`(step)                                             |    |
| `get_step_operator`(step)                                           |    |
| `initialize_tasks`()                                                |    |
| `set_downstream`(other)                                             |    |
| `set_upstream`(other)                                               |    |
| `setup_dag`()                                                       |    |
| `update_relative`(other[, upstream, edge_modifier])                 |    |

### Attributes

| `check_job`       |    |
|-------------------|----|
| `cleanup_nomad`   |    |
| `configure_nomad` |    |
| `leaves`          |    |
| `nomad_client`    |    |
| `register_job`    |    |
| `restart_job`     |    |
| `roots`           |    |
| `stop_job`        |    |
