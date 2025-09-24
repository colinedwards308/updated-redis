# backend/db_bootstrap.py
# backend/db_bootstrap.py
import pathlib
from sqlalchemy import text
from .db import engine

SCHEMA_PATH = pathlib.Path(__file__).with_name("ensure_schema.sql")
SCHEMA_SQL = SCHEMA_PATH.read_text()

def ensure_schema() -> None:
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))