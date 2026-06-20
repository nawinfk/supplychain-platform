import logging
import signal
import time
from datetime import UTC, datetime

import requests

from libs.config_common import get_settings
from libs.kafka_common import create_producer, delivery_callback
from libs.message_common import WeatherEvent
from libs.telemetry_common import correlation_context, setup_logging

TOPIC = "supplychain.raw.weather.v1"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
PORTS = [
    {"code": "USLAX", "name": "Los Angeles", "lat": 33.7405, "lon": -118.2728},
    {"code": "USLGB", "name": "Long Beach", "lat": 33.7542, "lon": -118.2165},
    {"code": "SGSIN", "name": "Singapore", "lat": 1.2644, "lon": 103.8400},
    {"code": "NLRTM", "name": "Rotterdam", "lat": 51.9496, "lon": 4.1453},
    {"code": "INMAA", "name": "Chennai", "lat": 13.0827, "lon": 80.2707},
]
running = True


def stop(*_args) -> None:
    global running
    running = False


def fetch_port(session: requests.Session, port: dict) -> WeatherEvent:
    response = session.get(
        OPEN_METEO_URL,
        params={
            "latitude": port["lat"],
            "longitude": port["lon"],
            "current": "temperature_2m,wind_speed_10m,weather_code",
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
        },
        timeout=15,
    )
    response.raise_for_status()
    raw = response.json()
    current = raw["current"]
    return WeatherEvent(
        port_code=port["code"],
        port_name=port["name"],
        lat=port["lat"],
        lon=port["lon"],
        temperature_c=current.get("temperature_2m"),
        wind_speed_kph=current.get("wind_speed_10m"),
        weather_code=current.get("weather_code"),
        event_time=current.get("time") or datetime.now(UTC),
        source="open-meteo.com",
        raw_payload=raw,
    )


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    producer = create_producer(settings)
    session = requests.Session()
    try:
        while running:
            started = time.monotonic()
            for port in PORTS:
                try:
                    event = fetch_port(session, port)
                    with correlation_context(port["code"]):
                        producer.produce(
                            TOPIC,
                            key=port["code"],
                            value=event.model_dump_json().encode(),
                            on_delivery=delivery_callback,
                        )
                        producer.poll(0)
                        logger.info("Published weather event port=%s", port["code"])
                except Exception:
                    logger.exception("Weather fetch failed port=%s", port["code"])
            delay = max(1, settings.weather_poll_interval_seconds - (time.monotonic() - started))
            time.sleep(delay)
    finally:
        producer.flush(10)
        session.close()


if __name__ == "__main__":
    main()
