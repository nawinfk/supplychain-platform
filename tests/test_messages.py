import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from libs.message_common import AisEvent, NewsEvent, WeatherEvent

SAMPLES = Path(__file__).parents[1] / "samples"


@pytest.mark.parametrize(
    ("filename", "schema"),
    [
        ("ais-event.json", AisEvent),
        ("weather-event.json", WeatherEvent),
        ("news-event.json", NewsEvent),
    ],
)
def test_sample_event_is_valid(filename, schema):
    payload = json.loads((SAMPLES / filename).read_text())
    assert schema.model_validate(payload)


def test_invalid_ais_coordinates_are_rejected():
    payload = json.loads((SAMPLES / "ais-event.json").read_text())
    payload["lat"] = 100
    with pytest.raises(ValidationError):
        AisEvent.model_validate(payload)
