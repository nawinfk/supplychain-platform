from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

from libs.db_common.models import Base, Vessel


def test_models_create_and_persist_with_sqlite():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    assert {
        "vessels",
        "vessel_positions",
        "port_weather",
        "supply_chain_news",
        "risk_events",
        "consumer_offsets_audit",
    }.issubset(set(inspect(engine).get_table_names()))

    with Session(engine) as session:
        session.add(Vessel(mmsi=123456789, vessel_name="TEST VESSEL"))
        session.commit()
        assert session.get(Vessel, 123456789).vessel_name == "TEST VESSEL"
