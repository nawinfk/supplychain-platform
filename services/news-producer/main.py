import hashlib
import logging
import signal
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import requests

from libs.config_common import get_settings
from libs.kafka_common import create_producer, delivery_callback
from libs.message_common import NewsEvent
from libs.telemetry_common import correlation_context, setup_logging

TOPIC = "supplychain.raw.news.v1"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
KEYWORDS = ["port strike", "congestion", "war", "flood", "delay", "customs"]
running = True


def stop(*_args) -> None:
    global running
    running = False


def parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    for parser in (
        lambda text: datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC),
        parsedate_to_datetime,
    ):
        try:
            return parser(value)
        except (ValueError, TypeError):
            pass
    return datetime.now(UTC)


def fetch_articles(session: requests.Session, attempts: int = 4) -> list[dict]:
    logger = logging.getLogger(__name__)
    query = " OR ".join(f'"{keyword}"' for keyword in KEYWORDS)
    for attempt in range(attempts):
        try:
            response = session.get(
                GDELT_URL,
                params={"query": query, "mode": "ArtList", "maxrecords": 100, "format": "json"},
                timeout=30,
            )
            response.raise_for_status()
            return response.json().get("articles", [])
        except (requests.RequestException, ValueError):
            if attempt == attempts - 1:
                raise
            delay = min(30, 2**attempt)
            logger.warning("GDELT request failed attempt=%s retry_in=%ss", attempt + 1, delay)
            time.sleep(delay)
    return []


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    producer = create_producer(settings)
    session = requests.Session()
    seen: set[str] = set()
    try:
        while running:
            try:
                for article in fetch_articles(session):
                    url = article.get("url")
                    title = article.get("title") or "Untitled supply chain report"
                    if not url:
                        continue
                    external_id = hashlib.sha256(url.encode()).hexdigest()
                    if external_id in seen:
                        continue
                    matched = [keyword for keyword in KEYWORDS if keyword in title.lower()]
                    event = NewsEvent(
                        external_id=external_id,
                        title=title,
                        url=url,
                        domain=article.get("domain"),
                        keywords=matched,
                        event_time=parse_time(article.get("seendate")),
                        source="gdeltproject.org",
                        raw_payload=article,
                    )
                    with correlation_context(external_id):
                        producer.produce(
                            TOPIC,
                            key=external_id,
                            value=event.model_dump_json().encode(),
                            on_delivery=delivery_callback,
                        )
                        producer.poll(0)
                    seen.add(external_id)
                if len(seen) > 10_000:
                    seen.clear()
            except Exception:
                logger.exception("GDELT poll failed; continuing after poll interval")
            time.sleep(settings.news_poll_interval_seconds)
    finally:
        producer.flush(10)
        session.close()


if __name__ == "__main__":
    main()
