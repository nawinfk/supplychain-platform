from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class AisEvent(BaseModel):
    mmsi: int = Field(gt=0)
    vessel_name: str | None = None
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    speed_over_ground: float | None = Field(default=None, ge=0)
    course_over_ground: float | None = Field(default=None, ge=0, le=360)
    event_time: datetime
    source: str
    raw_payload: dict[str, Any]


class WeatherEvent(BaseModel):
    port_code: str
    port_name: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    temperature_c: float | None = None
    wind_speed_kph: float | None = Field(default=None, ge=0)
    weather_code: int | None = None
    event_time: datetime
    source: str
    raw_payload: dict[str, Any]


class NewsEvent(BaseModel):
    external_id: str
    title: str
    url: HttpUrl
    domain: str | None = None
    keywords: list[str]
    event_time: datetime
    source: str
    raw_payload: dict[str, Any]
