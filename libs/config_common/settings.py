from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: str = "local"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "SASL_SSL"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = Field(
        default="", validation_alias=AliasChoices("KAFKA_API_KEY", "KAFKA_SASL_USERNAME")
    )
    kafka_sasl_password: str = Field(
        default="", validation_alias=AliasChoices("KAFKA_API_SECRET", "KAFKA_SASL_PASSWORD")
    )
    kafka_client_id: str = "supplychain-platform"
    kafka_auto_offset_reset: str = "earliest"

    database_url: str = (
        "postgresql+psycopg2://supplychain:supplychain@localhost:5432/supplychain"
    )
    aisstream_api_key: str = ""
    ais_mock_mode: bool = True
    ais_mock_interval_seconds: int = Field(default=5, ge=1)
    weather_poll_interval_seconds: int = Field(default=300, ge=1)
    news_poll_interval_seconds: int = Field(default=300, ge=1)
    risk_wind_threshold_kph: float = 50.0
    risk_low_speed_threshold_knots: float = 1.5
    risk_port_radius_km: float = 25.0
    log_level: str = "INFO"
    otel_service_name: str = "supplychain-platform"
    otel_exporter_otlp_endpoint: str = ""
    topic_ais_raw: str = "supplychain.raw.ais.v1"
    topic_weather_raw: str = "supplychain.raw.weather.v1"
    topic_news_raw: str = "supplychain.raw.news.v1"
    topic_risk_events: str = "supplychain.processed.risk-events.v1"
    topic_dlq: str = "supplychain.dlq.v1"

    @property
    def kafka_dry_run(self) -> bool:
        return self.env.lower() == "local" and self.kafka_sasl_username == "dummy"

    def kafka_config(self) -> dict[str, str]:
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "client.id": self.kafka_client_id,
        }
        if self.kafka_sasl_username:
            config.update(
                {
                    "sasl.mechanism": self.kafka_sasl_mechanism,
                    "sasl.username": self.kafka_sasl_username,
                    "sasl.password": self.kafka_sasl_password,
                }
            )
        return config


@lru_cache
def get_settings() -> Settings:
    return Settings()
