# backend/clean_sample_data.py
from __future__ import annotations
import re, uuid, random
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

rng = random.Random(42)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "datafiles"

CUSTOMERS_IN  = DATA / "customers_expanded.csv"
TX_IN         = DATA / "transactions_expanded.csv"
CUSTOMERS_OUT = DATA / "customers_clean.csv"
TX_OUT        = DATA / "transactions_clean.csv"

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def is_uuid(v: str) -> bool:
    try:
        uuid.UUID(str(v).strip())
        return True
    except Exception:
        return False

def synth_time(days_back_max: int = 90) -> str:
    # random time within last N days, ISO8601 UTC
    days_back = rng.randint(0, days_back_max)
    seconds = rng.randint(0, 24*3600-1)
    dt = datetime.now(tz=timezone.utc) - timedelta(days=days_back, seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")

def parse_or_synth_ts(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return synth_time()
    try:
        dt = pd.to_datetime(s, errors="raise", utc=True)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return synth_time()

def coerce_int(v, default=1, min_v=1, max_v=10**9):
    try:
        x = int(float(str(v).replace(",", "").strip()))
        if x < min_v: x = min_v
        if x > max_v: x = max_v
        return x
    except Exception:
        return default

def coerce_float(v, default=1.0, min_v=0.01, max_v=10**9):
    try:
        x = float(str(v).replace(",", "").strip())
        if x < min_v: x = min_v
        if x > max_v: x = max_v
        return round(x, 4)
    except Exception:
        return default

def is_email(v: str) -> bool:
    s = (v or "").strip()
    return bool(EMAIL_RE.match(s))

def main():
    # ---------- Customers ----------
    cust = pd.read_csv(CUSTOMERS_IN, dtype=str, keep_default_na=False)
    need_cust_cols = ["id","first_name","last_name","email","address","city","state","zip4","age"]
    for c in need_cust_cols:
        if c not in cust.columns:
            cust[c] = ""

    # Normalize / validate
    cust["id"] = cust["id"].astype(str).str.strip()
    cust = cust[cust["id"].map(is_uuid)]
    # Email must be non-empty & syntactically valid
    cust["email"] = cust["email"].astype(str).str.strip().str.lower()
    cust = cust[cust["email"].map(is_email)]
    # Dedup ids
    cust = cust.drop_duplicates(subset=["id"], keep="first").reset_index(drop=True)

    # Canonical order
    cust = cust[["id","first_name","last_name","email","address","city","state","zip4","age"]]

    # Build valid id pool for fixing tx
    valid_ids = cust["id"].tolist()
    if not valid_ids:
        raise SystemExit("No valid customers remain after cleaning; cannot proceed.")

    # ---------- Transactions ----------
    tx = pd.read_csv(TX_IN, dtype=str, keep_default_na=False)

    # Ensure all needed columns exist
    need_tx_cols = [
        "transaction_id","timestamp","user_id",
        "first_name","last_name","email","address",
        "category_l1","category_l2","category_l3",
        "quantity","unit_price","total_price",
    ]
    for c in need_tx_cols:
        if c not in tx.columns:
            tx[c] = ""

    # Trim basics
    for col in ["transaction_id","timestamp","user_id","email","first_name","last_name","address",
                "category_l1","category_l2","category_l3","quantity","unit_price","total_price"]:
        tx[col] = tx[col].astype(str).str.strip()

    # ---- Repair instead of drop ----
    # transaction_id: make valid UUID
    tx["transaction_id"] = tx["transaction_id"].apply(
        lambda v: str(uuid.uuid4()) if not is_uuid(v) else v
    )

    # user_id: must be valid UUID; if not, make it random valid customer id
    tx["user_id"] = tx["user_id"].apply(
        lambda v: v if is_uuid(v) else rng.choice(valid_ids)
    )
    # user_id must exist in customers; reassign unknowns
    valid_set = set(valid_ids)
    tx["user_id"] = tx["user_id"].apply(
        lambda v: v if v in valid_set else rng.choice(valid_ids)
    )

    # timestamp: parse or synthesize
    tx["timestamp"] = tx["timestamp"].apply(parse_or_synth_ts)

    # quantity/unit_price: coerce
    tx["quantity"] = tx["quantity"].apply(lambda q: coerce_int(q, default=1, min_v=1, max_v=1000))
    tx["unit_price"] = tx["unit_price"].apply(lambda p: coerce_float(p, default=1.0, min_v=0.01, max_v=1e6))
    tx["total_price"] = (tx["quantity"].astype(int) * tx["unit_price"].astype(float)).round(2)

    # Drop only truly hopeless rows (should be extremely rare after repairs)
    keep_mask = (
        tx["transaction_id"].map(is_uuid) &
        tx["user_id"].map(is_uuid)
    )
    tx = tx[keep_mask].copy()

    # Deduplicate transaction_id (keep first)
    tx = tx.drop_duplicates(subset=["transaction_id"], keep="first").reset_index(drop=True)

    # Canonical order
    tx = tx[[
        "transaction_id","timestamp","user_id",
        "first_name","last_name","email","address",
        "category_l1","category_l2","category_l3",
        "quantity","unit_price","total_price",
    ]]

    # ---- Save ----
    CUSTOMERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    cust.to_csv(CUSTOMERS_OUT, index=False)
    tx.to_csv(TX_OUT, index=False)

    print(f"Customers kept: {len(cust):,} -> {CUSTOMERS_OUT}")
    print(f"Transactions kept: {len(tx):,} -> {TX_OUT}")

if __name__ == "__main__":
    main()