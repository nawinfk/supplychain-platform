import json
import logging
import signal

from confluent_kafka import KafkaError
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert

from libs.config_common import get_settings
from libs.db_common import create_db_engine, session_factory
from libs.db_common.models import ConsumerOffsetAudit, Vessel, VesselPosition
from libs.kafka_common import create_consumer, create_producer, publish_to_dlq
from libs.message_common import AisEvent
from libs.telemetry_common import correlation_context, setup_logging

TOPIC = "supplychain.raw.ais.v1"
GROUP_ID = "supplychain.vessel-consumer.v1"
running = True


def stop(*_args) -> None:
    global running
    running = False


def persist_event(session, event: AisEvent, message) -> None:
    statement = insert(Vessel).values(
        mmsi=event.mmsi,
        vessel_name=event.vessel_name,
        first_seen_at=event.event_time,
        last_seen_at=event.event_time,
    )
    statement = statement.on_conflict_do_update(
        index_elements=[Vessel.mmsi],
        set_={"vessel_name": event.vessel_name, "last_seen_at": event.event_time},
    )
    session.execute(statement)
    session.add(
        VesselPosition(
            mmsi=event.mmsi,
            lat=event.lat,
            lon=event.lon,
            speed_over_ground=event.speed_over_ground,
            course_over_ground=event.course_over_ground,
            event_time=event.event_time,
            source=event.source,
            raw_payload=event.raw_payload,
        )
    )
    session.add(
        ConsumerOffsetAudit(
            consumer_group=GROUP_ID,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            status="db_committed",
        )
    )


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    engine = create_db_engine(settings.database_url)
    sessions = session_factory(engine)
    consumer = create_consumer(settings, GROUP_ID, [TOPIC])
    dlq_producer = create_producer(settings)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consume error: %s", message.error())
                continue
            try:
                payload = json.loads(message.value())
                event = AisEvent.model_validate(payload)
            except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as error:
                logger.warning("Invalid AIS event sent to DLQ: %s", error)
                publish_to_dlq(dlq_producer, message, error)
                consumer.commit(message=message, asynchronous=False)
                continue
            try:
                with correlation_context(str(event.mmsi)), sessions.begin() as session:
                    persist_event(session, event, message)
                consumer.commit(message=message, asynchronous=False)
                logger.info("Stored AIS position mmsi=%s offset=%s", event.mmsi, message.offset())
            except Exception:
                logger.exception("Database write failed; Kafka offset remains uncommitted")
    finally:
        consumer.close()
        dlq_producer.flush(10)
        engine.dispose()


if __name__ == "__main__":
    main()
