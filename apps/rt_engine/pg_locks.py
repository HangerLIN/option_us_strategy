"""PostgreSQL advisory lock for Top5 job deduplication."""
from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PGDayLock:
    """PostgreSQL advisory lock for same-day job mutual exclusion."""
    
    def __init__(self, session: Session, trade_date: str) -> None:
        self.sess = session
        self.key = f"top5:{trade_date}"
    
    def __enter__(self) -> "PGDayLock":
        # pg_try_advisory_lock(hashtext(key)) - locks within current database connection
        ok = self.sess.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:k))"), 
            {"k": self.key}
        ).scalar()
        if not ok:
            raise RuntimeError(f"job_locked:{self.key}")
        return self
    
    def __exit__(self, exc_type, exc, tb) -> None:
        self.sess.execute(
            text("SELECT pg_advisory_unlock(hashtext(:k))"), 
            {"k": self.key}
        )


@contextmanager
def top5_day_lock(session: Session, trade_date: str):
    """Context manager for Top5 same-day mutual exclusion."""
    lock = PGDayLock(session, trade_date)
    try:
        lock.__enter__()
        yield lock
    finally:
        lock.__exit__(None, None, None)

