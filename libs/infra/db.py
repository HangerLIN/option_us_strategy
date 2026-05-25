from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from libs.core.config import Settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_engine(settings: Settings) -> Engine:
    """Initialise (or return cached) SQLAlchemy engine."""
    global _engine
    if _engine is None:
        connect_args = {}
        if settings.database_url.startswith(("postgresql://", "postgresql+psycopg://")):
            connect_args["options"] = "-c timezone=America/New_York"
        _engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args=connect_args,
            future=True,
        )
    return _engine


def get_session_factory(settings: Settings) -> sessionmaker[Session]:
    """Return a session factory bound to the configured engine."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine(settings)
        _session_factory = sessionmaker(engine, expire_on_commit=False, future=True)
    return _session_factory


@contextmanager
def db_session(settings: Settings) -> Iterator[Session]:
    """Context manager yielding a transactional session."""
    session_factory = get_session_factory(settings)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
