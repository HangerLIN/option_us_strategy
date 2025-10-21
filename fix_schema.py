from libs.core import get_settings
from sqlalchemy import create_engine, text

engine = create_engine(get_settings().database_url)
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE bt_signals ALTER COLUMN accepted DROP NOT NULL"))
    conn.execute(text("ALTER TABLE bt_signals ALTER COLUMN reason DROP NOT NULL"))
    print("✓ Schema updated: accepted and reason columns now allow NULL")

