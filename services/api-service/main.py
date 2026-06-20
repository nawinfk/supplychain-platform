from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from libs.config_common import get_settings
from libs.db_common import create_db_engine, session_factory
from libs.db_common.models import RiskEvent, Vessel, VesselPosition
from libs.telemetry_common import setup_logging

settings = get_settings()
setup_logging(settings.log_level)
engine = create_db_engine(settings.database_url)
SessionLocal = session_factory(engine)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    engine.dispose()


app = FastAPI(
    title="Supply Chain Platform API",
    version="0.1.0",
    description="Real-time vessel and supply-chain risk query API",
    lifespan=lifespan,
)


def get_db():
    with SessionLocal() as session:
        yield session


DatabaseSession = Annotated[Session, Depends(get_db)]


def position_dict(position: VesselPosition | None):
    if not position:
        return None
    return {
        "lat": position.lat,
        "lon": position.lon,
        "speed_over_ground": position.speed_over_ground,
        "course_over_ground": position.course_over_ground,
        "event_time": position.event_time,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready(db: DatabaseSession):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception as error:
        raise HTTPException(status_code=503, detail="database unavailable") from error


@app.get("/vessels")
def vessels(
    db: DatabaseSession,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    rows = db.scalars(
        select(Vessel).order_by(desc(Vessel.last_seen_at)).limit(limit).offset(offset)
    )
    result = []
    for vessel in rows:
        latest = db.scalar(
            select(VesselPosition)
            .where(VesselPosition.mmsi == vessel.mmsi)
            .order_by(desc(VesselPosition.event_time))
            .limit(1)
        )
        result.append(
            {
                "mmsi": vessel.mmsi,
                "vessel_name": vessel.vessel_name,
                "last_seen_at": vessel.last_seen_at,
                "latest_position": position_dict(latest),
            }
        )
    return result


@app.get("/vessels/{mmsi}")
def vessel_detail(mmsi: int, db: DatabaseSession):
    vessel = db.get(Vessel, mmsi)
    if not vessel:
        raise HTTPException(status_code=404, detail="vessel not found")
    positions = db.scalars(
        select(VesselPosition)
        .where(VesselPosition.mmsi == mmsi)
        .order_by(desc(VesselPosition.event_time))
        .limit(100)
    )
    return {
        "mmsi": vessel.mmsi,
        "vessel_name": vessel.vessel_name,
        "first_seen_at": vessel.first_seen_at,
        "last_seen_at": vessel.last_seen_at,
        "positions": [position_dict(position) for position in positions],
    }


@app.get("/risk-events")
def risk_events(
    db: DatabaseSession,
    limit: int = Query(default=100, ge=1, le=1000),
    severity: str | None = None,
):
    query = select(RiskEvent).order_by(desc(RiskEvent.event_time)).limit(limit)
    if severity:
        query = query.where(RiskEvent.severity == severity)
    return [
        {
            "id": item.id,
            "risk_type": item.risk_type,
            "severity": item.severity,
            "entity_type": item.entity_type,
            "entity_id": item.entity_id,
            "description": item.description,
            "event_time": item.event_time,
            "details": item.details,
        }
        for item in db.scalars(query)
    ]


@app.get("/metrics-summary")
def metrics_summary(db: DatabaseSession):
    return {
        "vessel_count": db.scalar(select(func.count()).select_from(Vessel)),
        "position_count": db.scalar(select(func.count()).select_from(VesselPosition)),
        "risk_event_count": db.scalar(select(func.count()).select_from(RiskEvent)),
        "high_risk_count": db.scalar(
            select(func.count()).select_from(RiskEvent).where(RiskEvent.severity == "high")
        ),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
