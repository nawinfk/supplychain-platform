from libs.config_common.settings import Settings


def test_settings_load_from_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example:9092")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "test-key")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "test-secret")

    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "broker.example:9092"
    assert settings.kafka_config()["sasl.username"] == "test-key"
    assert settings.kafka_config()["sasl.password"] == "test-secret"


def test_kafka_config_omits_sasl_credentials_when_username_empty():
    settings = Settings(_env_file=None, kafka_sasl_username="", kafka_sasl_password="")
    assert "sasl.username" not in settings.kafka_config()


def test_requested_kafka_aliases_enable_local_dry_run(monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("KAFKA_API_KEY", "dummy")
    monkeypatch.setenv("KAFKA_API_SECRET", "dummy")

    settings = Settings(_env_file=None)

    assert settings.kafka_config()["sasl.username"] == "dummy"
    assert settings.kafka_dry_run is True
