import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer

from libs.config_common import Settings

logger = logging.getLogger(__name__)


class LocalProducer:
    """Producer-compatible local sink used when Kafka credentials are dummy."""

    def produce(self, topic: str, **kwargs: Any) -> None:
        logger.info("Kafka dry run: topic=%s key=%s", topic, kwargs.get("key"))

    def poll(self, _timeout: float) -> int:
        return 0

    def flush(self, _timeout: float | None = None) -> int:
        return 0


def delivery_callback(error: KafkaError | None, message: Message) -> None:
    if error:
        logger.error("Kafka delivery failed: %s", error)
    else:
        logger.debug(
            "Kafka delivery topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def create_producer(settings: Settings) -> Producer | LocalProducer:
    if settings.kafka_dry_run:
        logger.warning("Kafka publishing is in local dry-run mode")
        return LocalProducer()
    return Producer(settings.kafka_config())


def create_consumer(settings: Settings, group_id: str, topics: list[str]) -> Consumer:
    if settings.kafka_dry_run:
        raise RuntimeError(
            "Kafka consumer requires a running broker; replace the dummy local Kafka settings"
        )
    config = settings.kafka_config() | {
        "group.id": group_id,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": False,
    }
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer
