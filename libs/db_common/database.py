from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def create_db_engine(database_url: str, **kwargs) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, **kwargs)


def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
