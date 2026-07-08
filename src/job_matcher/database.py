from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
import logging

import psycopg2
from pgvector.psycopg import register_vector as register_vector_psycopg
from pgvector.psycopg2 import register_vector as register_vector_psycopg2
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from job_matcher.config import Settings, get_settings
from job_matcher.models import Base

logger = logging.getLogger(__name__)


def build_engine(settings: Settings | None = None) -> Engine:
    active_settings = settings or get_settings()
    return _cached_engine(active_settings.database_url)


@lru_cache(maxsize=4)
def _cached_engine(database_url: str) -> Engine:
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def register_vector_extension(dbapi_connection, _connection_record) -> None:
        try:
            if database_url.startswith("postgresql+psycopg2://"):
                register_vector_psycopg2(dbapi_connection)
                return
            register_vector_psycopg(dbapi_connection)
        except psycopg2.ProgrammingError as exc:
            if "vector type not found" not in str(exc):
                raise
            logger.info(
                "pgvector type is not available yet on this connection; continuing until CREATE EXTENSION runs"
            )

    return engine


@contextmanager
def session_scope(settings: Settings | None = None):
    engine = build_engine(settings)
    factory = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ensure_database(settings: Settings | None = None) -> None:
    engine = build_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.create_all(engine)
    engine.dispose()
