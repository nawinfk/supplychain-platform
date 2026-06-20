import json
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Message, Producer

from .client import delivery_callback

DLQ_TOPIC = "supplychain.dlq.v1"


def publish_to_dlq(
    producer: Producer, message: Message, error: Exception | str, payload: Any = None
) -> None:
    envelope = {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "error": str(error),
        "failed_at": datetime.now(UTC).isoformat(),
        "payload": payload if payload is not None else message.value().decode("utf-8", "replace"),
    }
    producer.produce(
        DLQ_TOPIC,
        key=message.key(),
        value=json.dumps(envelope, default=str).encode(),
        on_delivery=delivery_callback,
    )
    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"Failed to deliver {remaining} DLQ message(s)")
