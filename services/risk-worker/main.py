import json
import logging
import math
import signal
from dataclasses import dataclass

from confluent_kafka import KafkaError
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert

from libs.config_common import get_settings
from libs.db_common import create_db_engine, session_factory
from libs.db_common.models import PortWeather, RiskEvent, SupplyChainNews
from libs.kafka_common import create_consumer, create_producer, delivery_callback, publish_to_dlq
from libs.message_common import AisEvent, NewsEvent, WeatherEvent
from libs.telemetry_common import correlation_context, setup_logging

AIS_TOPIC = "supplychain.raw.ais.v1"
WEATHER_TOPIC = "supplychain.raw.weather.v1"
NEWS_TOPIC = "supplychain.raw.news.v1"
OUTPUT_TOPIC = "supplychain.processed.risk-events.v1"
GROUP_ID = "supplychain.risk-worker.v1"
PORTS = {
    "USLAX": (33.7405, -118.2728),
    "USLGB": (33.7542, -118.2165),
    "SGSIN": (1.2644, 103.8400),
    "NLRTM": (51.9496, 4.1453),
    "INMAA": (13.0827, 80.2707),
}
DISRUPTION_KEYWORDS = {"port strike", "congestion", "war", "flood", "delay", "customs"}
running = True


@dataclass
class RiskCandidate:
    risk_type: str
    severity: str
    entity_type: str
    entity_id: str
    description: str
    event_time: object
    details: dict


def stop(*_args) -> None:
    global running
    running = False


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(value))


def handle_weather(session, event: WeatherEvent, settings) -> RiskCandidate | None:
    session.add(
        PortWeather(
            port_code=event.port_code,
            port_name=event.port_name,
            lat=event.lat,
            lon=event.lon,
            temperature_c=event.temperature_c,
            wind_speed_kph=event.wind_speed_kph,
            weather_code=event.weather_code,
            event_time=event.event_time,
            raw_payload=event.raw_payload,
        )
    )
    if (
        event.wind_speed_kph is not None
        and event.wind_speed_kph >= settings.risk_wind_threshold_kph
    ):
        return RiskCandidate(
            "high_wind",
            "high",
            "port",
            event.port_code,
            f"High wind at {event.port_name}: {event.wind_speed_kph:.1f} km/h",
            event.event_time,
            {"wind_speed_kph": event.wind_speed_kph},
        )
    return None


def handle_news(session, event: NewsEvent) -> RiskCandidate | None:
    statement = (
        insert(SupplyChainNews)
        .values(
            external_id=event.external_id,
            title=event.title,
            url=str(event.url),
            domain=event.domain,
            keywords=event.keywords,
            event_time=event.event_time,
            raw_payload=event.raw_payload,
        )
        .on_conflict_do_nothing(index_elements=[SupplyChainNews.external_id])
    )
    session.execute(statement)
    matched = sorted(DISRUPTION_KEYWORDS.intersection({word.lower() for word in event.keywords}))
    if not matched:
        matched = sorted(word for word in DISRUPTION_KEYWORDS if word in event.title.lower())
    if matched:
        return RiskCandidate(
            "news_disruption",
            "medium",
            "news",
            event.external_id,
            f"Potential disruption reported: {event.title}",
            event.event_time,
            {"keywords": matched, "url": str(event.url)},
        )
    return None


def handle_ais(event: AisEvent, settings) -> RiskCandidate | None:
    if (
        event.speed_over_ground is None
        or event.speed_over_ground >= settings.risk_low_speed_threshold_knots
    ):
        return None
    nearby = [
        (code, haversine_km(event.lat, event.lon, lat, lon)) for code, (lat, lon) in PORTS.items()
    ]
    port_code, distance = min(nearby, key=lambda item: item[1])
    if distance > settings.risk_port_radius_km:
        return None
    return RiskCandidate(
        "low_vessel_speed",
        "medium",
        "vessel",
        str(event.mmsi),
        f"Vessel moving slowly near {port_code}: {event.speed_over_ground:.1f} knots",
        event.event_time,
        {"mmsi": event.mmsi, "port_code": port_code, "distance_km": round(distance, 2)},
    )


def parse_message(message):
    payload = json.loads(message.value())
    if message.topic() == AIS_TOPIC:
        return AisEvent.model_validate(payload)
    if message.topic() == WEATHER_TOPIC:
        return WeatherEvent.model_validate(payload)
    return NewsEvent.model_validate(payload)


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    engine = create_db_engine(settings.database_url)
    sessions = session_factory(engine)
    consumer = create_consumer(settings, GROUP_ID, [AIS_TOPIC, WEATHER_TOPIC, NEWS_TOPIC])
    producer = create_producer(settings)
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
                event = parse_message(message)
            except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as error:
                publish_to_dlq(producer, message, error)
                consumer.commit(message=message, asynchronous=False)
                continue
            try:
                with correlation_context(message.key().decode() if message.key() else None):
                    with sessions.begin() as session:
                        if isinstance(event, WeatherEvent):
                            candidate = handle_weather(session, event, settings)
                        elif isinstance(event, NewsEvent):
                            candidate = handle_news(session, event)
                        else:
                            candidate = handle_ais(event, settings)
                        risk = RiskEvent(**candidate.__dict__) if candidate else None
                        if risk:
                            session.add(risk)
                            session.flush()
                            output = {
                                "id": risk.id,
                                **candidate.__dict__,
                                "event_time": str(candidate.event_time),
                            }
                            producer.produce(
                                OUTPUT_TOPIC,
                                key=candidate.entity_id,
                                value=json.dumps(output).encode(),
                                on_delivery=delivery_callback,
                            )
                            if producer.flush(10):
                                raise RuntimeError("Risk event Kafka delivery timed out")
                    consumer.commit(message=message, asynchronous=False)
                    logger.info(
                        "Processed risk input topic=%s offset=%s", message.topic(), message.offset()
                    )
            except Exception:
                logger.exception("Risk processing failed; Kafka offset remains uncommitted")
    finally:
        consumer.close()
        producer.flush(10)
        engine.dispose()


if __name__ == "__main__":
    main()
