# backend/models.py
from __future__ import annotations

from sqlalchemy import (
    Column, String, Integer, Float, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from .db import Base  # IMPORTANT: use the SAME Base created in backend.db


class Customer(Base):
    __tablename__ = "customers"

    id = Column(String, primary_key=True, index=True)  # user_id from CSV
    first_name = Column(String, nullable=True)
    last_name  = Column(String, nullable=True)
    email      = Column(String, nullable=True, index=True)
    address    = Column(String, nullable=True)
    city       = Column(String, nullable=True)
    state      = Column(String, nullable=True)
    zip4       = Column(String, nullable=True)
    age        = Column(Integer, nullable=True)

    transactions = relationship("Transaction", back_populates="customer", cascade="all, delete-orphan")


class Transaction(Base):
    __tablename__ = "transactions"

    id          = Column(String, primary_key=True, index=True)  # transaction_id from CSV
    timestamp   = Column(DateTime(timezone=True), nullable=True)
    user_id     = Column(String, ForeignKey("customers.id", ondelete="CASCADE"), index=True)

    # denormalized convenience fields from CSV
    first_name  = Column(String, nullable=True)
    last_name   = Column(String, nullable=True)
    email       = Column(String, nullable=True)
    address     = Column(String, nullable=True)

    category_l1 = Column(String, nullable=True)
    category_l2 = Column(String, nullable=True)
    category_l3 = Column(String, nullable=True)

    quantity    = Column(Integer, nullable=False, default=0)
    unit_price  = Column(Float,   nullable=False, default=0.0)
    total_price = Column(Float,   nullable=False, default=0.0)

    customer = relationship("Customer", back_populates="transactions")


# Helpful indexes
Index("ix_transactions_user_time", Transaction.user_id, Transaction.timestamp)
Index("ix_transactions_time", Transaction.timestamp)