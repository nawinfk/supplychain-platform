from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    pass


JSONPayload = JSON().with_variant(JSONB, "postgresql")


class Vessel(Base):
    __tablename__ = "vessels"

    mmsi: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vessel_name: Mapped[str | None] = mapped_column(String(255))
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    positions: Mapped[list["VesselPosition"]] = relationship(back_populates="vessel")


class VesselPosition(Base):
    __tablename__ = "vessel_positions"
    __table_args__ = (
        Index("ix_vessel_positions_mmsi_event_time", "mmsi", "event_time"),
        Index("ix_vessel_positions_event_time", "event_time"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    mmsi: Mapped[int] = mapped_column(ForeignKey("vessels.mmsi", ondelete="CASCADE"))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    speed_over_ground: Mapped[float | None] = mapped_column(Float)
    course_over_ground: Mapped[float | None] = mapped_column(Float)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source: Mapped[str] = mapped_column(String(64))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONPayload)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    vessel: Mapped[Vessel] = relationship(back_populates="positions")


class PortWeather(Base):
    __tablename__ = "port_weather"
    __table_args__ = (Index("ix_port_weather_port_event_time", "port_code", "event_time"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    port_code: Mapped[str] = mapped_column(String(32))
    port_name: Mapped[str] = mapped_column(String(255))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    temperature_c: Mapped[float | None] = mapped_column(Float)
    wind_speed_kph: Mapped[float | None] = mapped_column(Float)
    weather_code: Mapped[int | None] = mapped_column(Integer)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONPayload)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SupplyChainNews(Base):
    __tablename__ = "supply_chain_news"
    __table_args__ = (Index("ix_supply_chain_news_event_time", "event_time"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(512), unique=True)
    title: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    domain: Mapped[str | None] = mapped_column(String(255))
    keywords: Mapped[list[str]] = mapped_column(JSONPayload)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONPayload)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RiskEvent(Base):
    __tablename__ = "risk_events"
    __table_args__ = (
        Index("ix_risk_events_event_time", "event_time"),
        Index("ix_risk_events_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    risk_type: Mapped[str] = mapped_column(String(64))
    severity: Mapped[str] = mapped_column(String(32))
    entity_type: Mapped[str] = mapped_column(String(32))
    entity_id: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(Text)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, Any]] = mapped_column(JSONPayload)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ConsumerOffsetAudit(Base):
    __tablename__ = "consumer_offsets_audit"
    __table_args__ = (
        Index("ix_consumer_offsets_group_topic", "consumer_group", "topic"),
        Index("ix_consumer_offsets_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    consumer_group: Mapped[str] = mapped_column(String(255))
    topic: Mapped[str] = mapped_column(String(255))
    partition: Mapped[int] = mapped_column(Integer)
    offset: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
