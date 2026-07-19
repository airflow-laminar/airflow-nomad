from datetime import timedelta

from nomad_pydantic import NomadConfiguration

from airflow_nomad import NomadAirflowConfiguration


def test_airflow_configuration_defaults(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    assert nomad_airflow_configuration.check_interval == timedelta(seconds=5)
    assert nomad_airflow_configuration.check_timeout == timedelta(hours=8)
    assert nomad_airflow_configuration.stop_on_exit is True
    assert nomad_airflow_configuration.cleanup is True
    assert nomad_airflow_configuration.purge_on_exit is False


def test_airflow_configuration_roundtrip(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    value = NomadAirflowConfiguration.model_validate_json(nomad_airflow_configuration.model_dump_json())

    assert value == nomad_airflow_configuration


def test_nomad_json_excludes_airflow_fields(nomad_airflow_configuration: NomadAirflowConfiguration) -> None:
    value = NomadConfiguration.model_validate_json(nomad_airflow_configuration.nomad_json())

    assert value.job == nomad_airflow_configuration.job
    assert "check_interval" not in nomad_airflow_configuration.nomad_json()
