# backend/db.py
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

# Read DATABASE_URL from env (falls back to local Postgres; adjust if you prefer SQLite during dev)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://redisdemo:redisdemo@localhost:5432/redisdemo",
)

# SQLAlchemy 2.x engine
engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,       # avoid stale connections
)

# Session factory
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)

# Declarative base exported for models.py
Base = declarative_base()