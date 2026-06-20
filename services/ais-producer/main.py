import json
import logging
import random
import signal
import time
from datetime import UTC, datetime

from websockets.sync.client import connect

from libs.config_common import get_settings
from libs.kafka_common import create_producer, delivery_callback
from libs.message_common import AisEvent
from libs.telemetry_common import correlation_context, setup_logging

AIS_URL = "wss://stream.aisstream.io/v0/stream"
running = True


def stop(*_args) -> None:
    global running
    running = False


def publish(producer, event: AisEvent, topic: str) -> None:
    producer.produce(
        topic,
        key=str(event.mmsi),
        value=event.model_dump_json().encode(),
        on_delivery=delivery_callback,
    )
    producer.poll(0)


def mock_events(producer, interval: int, topic: str) -> None:
    logger = logging.getLogger(__name__)
    vessels = [
        (367175150, "PACIFIC TRADER", 33.74, -118.27),
        (563012345, "ASIA STAR", 1.26, 103.84),
    ]
    logger.warning("AISSTREAM_API_KEY is empty; running AIS producer in mock mode")
    while running:
        mmsi, name, lat, lon = random.choice(vessels)
        raw = {"mock": True, "generated_at": datetime.now(UTC).isoformat()}
        event = AisEvent(
            mmsi=mmsi,
            vessel_name=name,
            lat=lat + random.uniform(-0.02, 0.02),
            lon=lon + random.uniform(-0.02, 0.02),
            speed_over_ground=round(random.uniform(0.3, 16), 1),
            course_over_ground=round(random.uniform(0, 360), 1),
            event_time=datetime.now(UTC),
            source="aisstream-mock",
            raw_payload=raw,
        )
        with correlation_context(str(mmsi)):
            publish(producer, event, topic)
            logger.info("Published mock AIS event mmsi=%s", mmsi)
        time.sleep(interval)


def live_events(producer, api_key: str, topic: str) -> None:
    logger = logging.getLogger(__name__)
    while running:
        ws = None
        try:
            ws = connect(AIS_URL, open_timeout=60)
            ws.send(json.dumps({"APIKey": api_key, "BoundingBoxes": [[[-90, -180], [90, 180]]]}))
            logger.info("Connected to AISStream")
            while running:
                raw = json.loads(ws.recv())
                if raw.get("MessageType") != "PositionReport":
                    continue
                meta = raw.get("MetaData", {})
                position = raw.get("Message", {}).get("PositionReport", {})
                event = AisEvent(
                    mmsi=int(meta.get("MMSI") or position["UserID"]),
                    vessel_name=(meta.get("ShipName") or "").strip() or None,
                    lat=position["Latitude"],
                    lon=position["Longitude"],
                    speed_over_ground=position.get("Sog"),
                    course_over_ground=position.get("Cog"),
                    event_time=meta.get("time_utc") or datetime.now(UTC),
                    source="aisstream.io",
                    raw_payload=raw,
                )
                with correlation_context(str(event.mmsi)):
                    publish(producer, event, topic)
        except Exception:
            logger.exception("AISStream connection failed; reconnecting in 10 seconds")
            time.sleep(10)
        finally:
            if ws:
                ws.close()


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    producer = create_producer(settings)
    try:
        if settings.aisstream_api_key and not settings.ais_mock_mode:
            live_events(producer, settings.aisstream_api_key, settings.topic_ais_raw)
        else:
            mock_events(producer, settings.ais_mock_interval_seconds, settings.topic_ais_raw)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
