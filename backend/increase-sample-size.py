# backend/increase-sample-size.py
import uuid, random, math
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import os

rng = random.Random(42)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "datafiles"

SRC_CUSTOMERS = DATA / "customers_with_id.csv"      # originals
SRC_TX        = DATA / "transactions.csv"

OUT_CUSTOMERS = DATA / "customers_expanded.csv"     # outputs
OUT_TX        = DATA / "transactions_expanded.csv"

TARGET_CUSTOMERS = 100_000          # ~20k customers
MIN_TX_PER_CUSTOMER = 5            # minimum transactions per customer
MAX_TX_PER_CUSTOMER = 20           # maximum transactions per customer

# ---------------- helpers ----------------
def colmap(df):
    """lowercase->actual column name map (trim spaces)."""
    return {str(c).strip().lower(): c for c in df.columns}

def pick(df, *candidates, default=None):
    """Pick a column (case-insensitive). Returns actual name or None."""
    m = colmap(df)
    for c in candidates:
        if c and str(c).strip().lower() in m:
            return m[str(c).strip().lower()]
    return default

def ensure_uuid(s) -> str:
    """Return valid UUID string. If s is invalid/missing -> new uuid4."""
    s = ("" if s is None else str(s)).strip()
    try:
        return str(uuid.UUID(s))
    except Exception:
        return str(uuid.uuid4())

def random_email() -> str:
    return f"user{rng.randint(100000,999999)}@example.com"

def normalize_email(e: str) -> str:
    e = (e or "").strip()
    if not e or e.lower() == "empty":
        return random_email()
    return e

def jitter_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return name
    # very light jitter to avoid dupes after expansion
    return f"{name}{rng.randint(1, 9999)}"

def synth_time():
    """Random UTC time within last 30 days, ISO 8601 Z-suffixed."""
    days_back = rng.randint(0, 30)  # Changed from 90 to 30 days
    seconds = rng.randint(0, 24*3600-1)
    dt = datetime.now(timezone.utc) - timedelta(days=days_back, seconds=seconds)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def safe_int(x, default=None):
    try:
        s = str(x).replace(",", "").strip()
        return int(float(s))
    except Exception:
        return default

def safe_float(x, default=None):
    try:
        s = str(x).replace(",", "").strip()
        return float(s)
    except Exception:
        return default

# ------------- load source (as strings, no NaN) -------------
customers_src = pd.read_csv(SRC_CUSTOMERS, dtype=str, keep_default_na=False)
tx_src        = pd.read_csv(SRC_TX, dtype=str, keep_default_na=False)

# --------- normalize CUSTOMERS to canonical schema ----------
# Detect possible source column names
c_id    = pick(customers_src, "id", "user_id", "customer_id")
c_fn    = pick(customers_src, "first_name", "firstname", "first", "person_first_name")
c_ln    = pick(customers_src, "last_name", "lastname", "last", "surname", "person_last_name")
c_email = pick(customers_src, "email", "e-mail", "em_email")
c_addr  = pick(customers_src, "address", "street", "addr", "primary_address")
c_city  = pick(customers_src, "city")
c_state = pick(customers_src, "state", "province")
c_zip4  = pick(customers_src, "zip4", "zip", "zipcode", "postal", "zip")
c_age   = pick(customers_src, "age", "aiq_age")

# Build canonical DataFrame
cust = pd.DataFrame({
    "id":        customers_src[c_id]    if c_id    else ["" for _ in range(len(customers_src))],
    "first_name":customers_src[c_fn]    if c_fn    else ["" for _ in range(len(customers_src))],
    "last_name": customers_src[c_ln]    if c_ln    else ["" for _ in range(len(customers_src))],
    "email":     customers_src[c_email] if c_email else ["" for _ in range(len(customers_src))],
    "address":   customers_src[c_addr]  if c_addr  else ["" for _ in range(len(customers_src))],
    "city":      customers_src[c_city]  if c_city  else ["" for _ in range(len(customers_src))],
    "state":     customers_src[c_state] if c_state else ["" for _ in range(len(customers_src))],
    "zip4":      customers_src[c_zip4]  if c_zip4  else ["" for _ in range(len(customers_src))],
    "age":       customers_src[c_age]   if c_age   else ["" for _ in range(len(customers_src))],
})

# Coerce/clean
cust["id"] = cust["id"].apply(ensure_uuid)
cust["email"] = cust["email"].apply(normalize_email)

# Make sure age becomes int-like string (keeping within 18..95); blanks allowed
clean_age = []
for v in cust["age"]:
    ai = safe_int(v, default=None)
    if ai is None:
        clean_age.append("")
    else:
        clean_age.append(str(max(18, min(95, ai))))
cust["age"] = clean_age

# Drop duplicate ids & reindex
cust = cust.drop_duplicates(subset=["id"]).reset_index(drop=True)

# ------------- expand customers up to TARGET_CUSTOMERS -------------
base_n = len(cust)
need = max(0, TARGET_CUSTOMERS - base_n)

new_rows = []
for i in range(need):
    base = cust.iloc[i % base_n].copy()

    # Always assign a fresh id + email
    base["id"] = str(uuid.uuid4())

    # Lightly jitter names to avoid obv duplicates
    base["first_name"] = jitter_name(base.get("first_name", ""))
    base["last_name"]  = jitter_name(base.get("last_name", ""))

    # Email: mutate to unique
    e = (base.get("email") or "").strip()
    if not e or e.lower() == "empty":
        base["email"] = random_email()
    else:
        user, _, domain = e.partition("@")
        base["email"] = f"{(user or 'user')}{rng.randint(1,999999)}@{domain or 'example.com'}"

    # Age: gentle jitter if present
    ai = safe_int(base.get("age", ""), default=None)
    if ai is not None:
        ai = max(18, min(95, ai + rng.randint(-2, 2)))
        base["age"] = str(ai)

    new_rows.append(base)

if new_rows:
    cust_expanded = pd.concat([cust, pd.DataFrame(new_rows)], ignore_index=True)
else:
    cust_expanded = cust.copy()

# Final pass: enforce valid unique UUIDs & non-empty emails
cust_expanded["id"] = cust_expanded["id"].apply(ensure_uuid)
cust_expanded["email"] = cust_expanded["email"].apply(normalize_email)
cust_expanded = cust_expanded.drop_duplicates(subset=["id"]).reset_index(drop=True)

# Top back up if duplicates dropped
seen_ids = set(cust_expanded["id"].tolist())
while len(cust_expanded) < TARGET_CUSTOMERS:
    base = cust_expanded.iloc[rng.randrange(len(cust_expanded))].copy()
    # fresh id/email
    new_id = str(uuid.uuid4())
    while new_id in seen_ids:
        new_id = str(uuid.uuid4())
    seen_ids.add(new_id)
    base["id"] = new_id
    base["email"] = random_email()
    cust_expanded = pd.concat([cust_expanded, pd.DataFrame([base])], ignore_index=True)

# Build quick lookup for transactions join
cust_lookup = {
    row["id"]: {
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "email": row["email"],
        "address": row["address"],
    }
    for _, row in cust_expanded.iterrows()
}
cust_ids = list(cust_lookup.keys())
cust_n = len(cust_ids)

# --------- IMPROVED TRANSACTION GENERATION ----------
# First, parse the source transaction data to get column mappings
# --------- CONFIG FOR DISTINCT ACTIVE USERS ----------
ACTIVE_CUSTOMERS = int(os.getenv("ACTIVE_CUSTOMERS", "10000"))  # how many distinct shoppers get tx
MIN_TX_PER_CUSTOMER = int(os.getenv("MIN_TX_PER_CUSTOMER", "3"))
MAX_TX_PER_CUSTOMER = int(os.getenv("MAX_TX_PER_CUSTOMER", "8"))
DAYS_BACK_MAX = int(os.getenv("DAYS_BACK_MAX", "30"))  # recent window for timestamps

def synth_time(days_back_max=DAYS_BACK_MAX):
    days_back = rng.randint(0, max(1, days_back_max))
    seconds = rng.randint(0, 24*3600-1)
    dt = datetime.now(timezone.utc) - timedelta(days=days_back, seconds=seconds)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

# --------- parse tx templates (unchanged idea, slightly hardened) ----------
tx = tx_src.copy()

t_txid   = pick(tx, "transaction_id", "id")
t_ts     = pick(tx, "timestamp", "time", "ts", "date")
t_uid    = pick(tx, "user_id", "customer_id", "id")
t_fn     = pick(tx, "first_name", "firstname", "first")
t_ln     = pick(tx, "last_name", "lastname", "last")
t_email  = pick(tx, "email", "e-mail")
t_addr   = pick(tx, "address", "addr", "primary_address")
t_cat1   = pick(tx, "category_l1", "cat_l1", "category1")
t_cat2   = pick(tx, "category_l2", "cat_l2", "category2")
t_cat3   = pick(tx, "category_l3", "cat_l3", "category3")
t_qty    = pick(tx, "quantity", "qty")
t_unit   = pick(tx, "unit_price", "price")

if len(tx) == 0:
    tx_template = [{
        "category_l1": "GROCERY",
        "category_l2": "GENERAL",
        "category_l3": "Misc Item",
        "quantity": 1,
        "unit_price": 5.99,
    }]
else:
    tx_template = []
    for _, row in tx.iterrows():
        template = {
            "category_l1": (row.get(t_cat1) or "GROCERY"),
            "category_l2": (row.get(t_cat2) or "GENERAL"),
            "category_l3": (row.get(t_cat3) or "Item"),
            "quantity": max(1, safe_int(row.get(t_qty, "1"), default=1)),
            "unit_price": max(0.01, safe_float(row.get(t_unit, "1.99"), default=1.99)),
        }
        tx_template.append(template)

print(f"Using {len(tx_template)} transaction templates")

# --------- choose ACTIVE_CUSTOMERS distinct users ----------
active_n = min(ACTIVE_CUSTOMERS, len(cust_ids))
active_ids = rng.sample(cust_ids, active_n)  # guarantees uniqueness

# --------- generate transactions for those users ----------
tx_rows = []
total_transactions_generated = 0

for i, customer_id in enumerate(active_ids, 1):
    num_transactions = rng.randint(MIN_TX_PER_CUSTOMER, MAX_TX_PER_CUSTOMER)
    customer_info = cust_lookup[customer_id]

    for _ in range(num_transactions):
        template = rng.choice(tx_template)
        qty = max(1, template["quantity"] + rng.randint(-1, 2))
        unit = round(template["unit_price"] * (0.8 + 0.4 * rng.random()), 2)

        tx_row = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": synth_time(),
            "user_id": customer_id,
            "first_name": customer_info.get("first_name", ""),
            "last_name":  customer_info.get("last_name", ""),
            "email":      customer_info.get("email", ""),
            "address":    customer_info.get("address", ""),
            "category_l1": template["category_l1"],
            "category_l2": template["category_l2"],
            "category_l3": template["category_l3"],
            "quantity": qty,
            "unit_price": unit,
        }
        tx_row["total_price"] = round(qty * unit, 2)
        tx_rows.append(tx_row)
        total_transactions_generated += 1

    if i % 1000 == 0:
        print(f"  generated for {i}/{active_n} customers…")

print(f"Generated {total_transactions_generated} transactions for {active_n} distinct customers")

tx_expanded = pd.DataFrame(tx_rows)
keep_cols = [
    "transaction_id","timestamp","user_id",
    "first_name","last_name","email","address",
    "category_l1","category_l2","category_l3",
    "quantity","unit_price","total_price",
]
tx_expanded = tx_expanded[keep_cols]

# ------------- save -------------
OUT_CUSTOMERS.parent.mkdir(parents=True, exist_ok=True)
cust_expanded.to_csv(OUT_CUSTOMERS, index=False)
tx_expanded.to_csv(OUT_TX, index=False)

print(f"Wrote {len(cust_expanded):,} customers -> {OUT_CUSTOMERS}")
print(f"Wrote {len(tx_expanded):,} transactions -> {OUT_TX}")

# Print distribution stats
tx_per_customer = tx_expanded.groupby('user_id').size()
print(f"Transaction distribution:")
print(f"  Min transactions per customer: {tx_per_customer.min()}")
print(f"  Max transactions per customer: {tx_per_customer.max()}")
print(f"  Average transactions per customer: {tx_per_customer.mean():.1f}")
print(f"  Customers with 1 transaction: {(tx_per_customer == 1).sum()}")
print(f"  Customers with 5+ transactions: {(tx_per_customer >= 5).sum()}")
print(f"  Customers with 10 transactions: {(tx_per_customer == 10).sum()}")