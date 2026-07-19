# API reference

The public integration API is re-exported from `airflow_nomad`. Jobspec models
are re-exported from `nomad_pydantic` and documented in its
[API reference](https://airflow-laminar.github.io/nomad-pydantic/docs/src/api.html).

## Airflow configuration and tasks

```{eval-rst}
.. currentmodule:: airflow_nomad

.. autosummary::
   :toctree: _build

   NomadAirflowConfiguration
   NomadTaskArgs
   NomadTask
   NomadOperatorArgs
   NomadOperator
   load_airflow_config
```

## Runtime lifecycle

```{eval-rst}
.. currentmodule:: airflow_nomad

.. autosummary::
   :toctree: _build

   Nomad
```
