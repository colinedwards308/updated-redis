# backend/seed.py
from __future__ import annotations

import csv
import re
import uuid
from pathlib import Path
from typing import Dict, Any, Iterable, List

from sqlalchemy import text
from .db import SessionLocal  # we derive an engine from this


# ----------------------- helpers -----------------------

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _str(x: Any) -> str:
    return ("" if x is None else str(x)).strip()

def _uuid_or_new(x: Any) -> str:
    s = _str(x)
    try:
        return str(uuid.UUID(s))
    except Exception:
        return str(uuid.uuid4())

def _clean_zip(z: Any) -> str:
    return _str(z).replace(",", "")

def _clean_int(x: Any, default: int = 0) -> int:
    s = _str(x).replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return default

def _clean_float(x: Any, default: float = 0.0) -> float:
    s = _str(x).replace(",", "")
    try:
        return float(s)
    except Exception:
        return default

def _safe_email(first: str, last: str, cid: str, given: str) -> str:
    e = _str(given)
    if not e or e.lower() == "empty" or not EMAIL_RE.match(e):
        left = f"{_str(first).lower()}.{_str(last).lower()}".strip(".") or "user"
        e = f"{left}.{cid[:8]}@example.com"
    return e

def _read_csv_rows(path: str | Path) -> Iterable[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k: _str(v) for k, v in row.items()}

def _colmap(fieldnames: list[str]) -> Dict[str, str]:
    return {c.strip().lower(): c for c in (fieldnames or [])}

def _pick(m: Dict[str, str], *cands: str) -> str | None:
    for c in cands:
        if c and c.lower() in m:
            return m[c.lower()]
    return None


# ----------------------- schema ops -----------------------

def create_schema() -> None:
    """
    Create minimal tables if they don't exist.
    Uses native SQL so we don't depend on ORM model metadata.
    """
    with SessionLocal() as session:
        # enable gen_random_uuid() when available; ignore if missing
        try:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        except Exception:
            pass

        session.execute(text("""
        CREATE TABLE IF NOT EXISTS customers (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name  TEXT,
            email      TEXT NOT NULL,
            address    TEXT,
            city       TEXT,
            state      TEXT,
            zip4       TEXT,
            age        INT
        );
        """))

        session.execute(text("""
        CREATE TABLE IF NOT EXISTS transactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            category_l1 TEXT,
            category_l2 TEXT,
            category_l3 TEXT,
            quantity INT,
            unit_price NUMERIC(12,2),
            total_price NUMERIC(14,2)
        );
        """))

        # helpful indexes
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_time ON transactions(timestamp)"))
        session.commit()


def wipe_all_data() -> None:
    with SessionLocal() as session:
        session.execute(text("TRUNCATE TABLE transactions, customers RESTART IDENTITY CASCADE"))
        session.commit()


# ----------------------- data load -----------------------

def load_from_csv(customers_csv: str | Path, transactions_csv: str | Path, reset: bool = True) -> Dict[str, int]:
    """
    Load customers & transactions from CSV files with strong sanitization:
    - Every customer gets a valid UUID 'id' (generate if blank/invalid)
    - Emails are synthesized if blank/'empty'/invalid
    - Duplicate customer IDs are dropped before insert
    - Transactions with missing/invalid/unknown user_id are skipped
    - Numeric fields coerced safely
    """
    customers_path = Path(customers_csv)
    tx_path = Path(transactions_csv)

    if not customers_path.is_file():
        raise FileNotFoundError(f"Customers CSV not found: {customers_path}")
    if not tx_path.is_file():
        raise FileNotFoundError(f"Transactions CSV not found: {tx_path}")

    if reset:
        wipe_all_data()
    # ensure schema exists
    create_schema()

    stats = {"customers_loaded": 0, "transactions_loaded": 0, "active_shoppers": 0, "products_loaded": 0}

    # -------- read & sanitize customers --------
    with open(customers_path, newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        fmap = _colmap(dr.fieldnames or [])

        c_id   = _pick(fmap, "id", "user_id", "customer_id")
        c_fn   = _pick(fmap, "first_name", "firstname", "first", "person_first_name")
        c_ln   = _pick(fmap, "last_name", "lastname", "last", "surname", "person_last_name")
        c_em   = _pick(fmap, "email", "e-mail", "em_email")
        c_addr = _pick(fmap, "address", "street", "primary_address")
        c_city = _pick(fmap, "city")
        c_st   = _pick(fmap, "state", "province")
        c_zip  = _pick(fmap, "zip4", "zip", "zipcode", "postal", "zip4")
        c_age  = _pick(fmap, "age", "aiq_age")

        seen_ids: set[str] = set()
        cust_rows: List[Dict[str, Any]] = []

        for raw in dr:
            cid = _uuid_or_new(raw.get(c_id) if c_id else "")
            if cid in seen_ids:
                continue

            first = raw.get(c_fn, "") if c_fn else ""
            last  = raw.get(c_ln, "") if c_ln else ""
            email = _safe_email(first, last, cid, raw.get(c_em, "") if c_em else "")

            cust_rows.append({
                "id": cid,
                "first_name": _str(first),
                "last_name": _str(last),
                "email": email,
                "address": _str(raw.get(c_addr, "") if c_addr else raw.get("address", "")),
                "city": _str(raw.get(c_city, "") if c_city else raw.get("city", "")),
                "state": _str(raw.get(c_st, "") if c_st else raw.get("state", "")),
                "zip4": _clean_zip(raw.get(c_zip, "") if c_zip else raw.get("zip4", "")),
                "age": _clean_int(raw.get(c_age, "") if c_age else raw.get("age", ""), default=0),
            })
            seen_ids.add(cid)

    # -------- insert customers in batches --------
    with SessionLocal() as session:
        if cust_rows:
            ins_c = text("""
                INSERT INTO customers (id, first_name, last_name, email, address, city, state, zip4, age)
                VALUES (:id, :first_name, :last_name, :email, :address, :city, :state, :zip4, :age)
                ON CONFLICT (id) DO NOTHING
            """)
            B = 5000
            for i in range(0, len(cust_rows), B):
                session.execute(ins_c, cust_rows[i:i+B])
                session.commit()
            stats["customers_loaded"] = len(cust_rows)

        valid_ids = {r["id"] for r in cust_rows}

        # -------- read & sanitize transactions --------
        with open(tx_path, newline="", encoding="utf-8") as f:
            dr = csv.DictReader(f)
            fmap = _colmap(dr.fieldnames or [])

            t_uid  = _pick(fmap, "user_id", "customer_id", "id")
            t_qty  = _pick(fmap, "quantity", "qty")
            t_unit = _pick(fmap, "unit_price", "price")
            t_tot  = _pick(fmap, "total_price", "total")
            t_l1   = _pick(fmap, "category_l1", "cat_l1", "category1")
            t_l2   = _pick(fmap, "category_l2", "cat_l2", "category2")
            t_l3   = _pick(fmap, "category_l3", "cat_l3", "category3")
            t_ts   = _pick(fmap, "timestamp", "time", "ts", "date")

            tx_rows: List[Dict[str, Any]] = []
            for raw in dr:
                uid = _uuid_or_new(raw.get(t_uid) if t_uid else "")
                if uid not in valid_ids:
                    # Skip tx that don't map to customers we just inserted.
                    continue

                qty  = _clean_int(raw.get(t_qty) if t_qty else 1, default=1)
                unit = _clean_float(raw.get(t_unit) if t_unit else 1.0, default=1.0)
                tot  = _clean_float(raw.get(t_tot) if t_tot else qty * unit, default=qty * unit)
                ts   = _str(raw.get(t_ts) if t_ts else "") or None  # let DB default if None

                tx_rows.append({
                    "user_id": uid,
                    "quantity": qty,
                    "unit_price": unit,
                    "total_price": tot,
                    "category_l1": _str(raw.get(t_l1) if t_l1 else ""),
                    "category_l2": _str(raw.get(t_l2) if t_l2 else ""),
                    "category_l3": _str(raw.get(t_l3) if t_l3 else ""),
                    "timestamp": ts,
                })

        if tx_rows:
            ins_t = text("""
                INSERT INTO transactions
                    (user_id, quantity, unit_price, total_price, category_l1, category_l2, category_l3, timestamp)
                VALUES
                    (:user_id, :quantity, :unit_price, :total_price, :category_l1, :category_l2, :category_l3, COALESCE(:timestamp, NOW()))
            """)
            B = 5000
            for i in range(0, len(tx_rows), B):
                session.execute(ins_t, tx_rows[i:i+B])
                session.commit()
            stats["transactions_loaded"] = len(tx_rows)

    return stats