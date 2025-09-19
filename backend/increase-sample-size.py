# backend/increase-sample-size.py
import uuid, random, math
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

rng = random.Random(42)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "datafiles"

SRC_CUSTOMERS = DATA / "customers_with_id.csv"      # originals
SRC_TX        = DATA / "transactions.csv"

OUT_CUSTOMERS = DATA / "customers_expanded.csv"     # outputs
OUT_TX        = DATA / "transactions_expanded.csv"

TARGET_CUSTOMERS = 20_000          # ~20k customers
TX_PER_CUSTOMER  = 5               # ~5 transactions each, on average

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
    """Random UTC time within last 90 days, ISO 8601 Z-suffixed."""
    days_back = rng.randint(0, 90)
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

# --------- normalize TRANSACTIONS to canonical schema ----------
tx = tx_src.copy()

t_txid   = pick(tx, "transaction_id", "id")
t_ts     = pick(tx, "timestamp", "time", "ts", "date")
t_uid    = pick(tx, "user_id", "customer_id", "id")  # we'll overwrite anyway
t_fn     = pick(tx, "first_name", "firstname", "first")
t_ln     = pick(tx, "last_name", "lastname", "last")
t_email  = pick(tx, "email", "e-mail")
t_addr   = pick(tx, "address", "addr", "primary_address")
t_cat1   = pick(tx, "category_l1", "cat_l1", "category1")
t_cat2   = pick(tx, "category_l2", "cat_l2", "category2")
t_cat3   = pick(tx, "category_l3", "cat_l3", "category3")
t_qty    = pick(tx, "quantity", "qty")
t_unit   = pick(tx, "unit_price", "price")
t_total  = pick(tx, "total_price", "total")

# Ensure numeric-ish cols are floats/ints where needed (used for jittering)
if t_qty:  tx[t_qty]  = pd.to_numeric(tx[t_qty],  errors="coerce")
if t_unit: tx[t_unit] = pd.to_numeric(tx[t_unit], errors="coerce")
if t_total:tx[t_total]= pd.to_numeric(tx[t_total],errors="coerce")

# Target rows
TARGET_TX = max(len(tx), cust_n * TX_PER_CUSTOMER)

# Expand / synthesize transactions by sampling source and reassigning user_id
mult = math.ceil(TARGET_TX / max(1, len(tx))) if len(tx) else 1
tx_rows = []

for m in range(mult):
    if len(tx) == 0:
        # create a tiny seed if source file is empty
        chunk = pd.DataFrame([{
            (t_cat1 or "category_l1"): "GROCERY",
            (t_cat2 or "category_l2"): "GENERAL",
            (t_cat3 or "category_l3"): "Misc Item",
            (t_qty  or "quantity"): 1,
            (t_unit or "unit_price"): round(1.0 + rng.random()*9.0, 2),
        }])
    else:
        chunk = tx.sample(frac=1.0, replace=True, random_state=42 + m).copy()

    # Ensure canonical fields exist in the chunk
    for need in [t_cat1 or "category_l1", t_cat2 or "category_l2", t_cat3 or "category_l3"]:
        if need not in chunk.columns:
            chunk[need] = ""

    # Assign a real customer to each row and copy customer fields
    user_ids = [cust_ids[rng.randrange(cust_n)] for _ in range(len(chunk))]
    chunk["user_id"] = user_ids  # canonical

    # Customer details
    chunk["first_name"] = [cust_lookup[uid]["first_name"] for uid in user_ids]
    chunk["last_name"]  = [cust_lookup[uid]["last_name"]  for uid in user_ids]
    chunk["email"]      = [cust_lookup[uid]["email"]      for uid in user_ids]
    chunk["address"]    = [cust_lookup[uid]["address"]    for uid in user_ids]

    # Quantity / unit_price / total_price
    if t_qty:
        chunk["quantity"] = chunk[t_qty].apply(lambda q: int(max(1, round(float(q)) if pd.notna(q) else 1)))
    else:
        chunk["quantity"] = 1

    if t_unit:
        chunk["unit_price"] = chunk[t_unit].apply(lambda p: round((float(p) if pd.notna(p) else 1.0) * (0.9 + 0.2 * rng.random()), 2))
    else:
        chunk["unit_price"] = round(1.0 + rng.random() * 9.0, 2)

    chunk["total_price"] = (chunk["unit_price"].astype(float) * chunk["quantity"].astype(int)).round(2)

    # Set categories to canonical names if needed
    if t_cat1 and t_cat1 != "category_l1": chunk.rename(columns={t_cat1: "category_l1"}, inplace=True)
    if t_cat2 and t_cat2 != "category_l2": chunk.rename(columns={t_cat2: "category_l2"}, inplace=True)
    if t_cat3 and t_cat3 != "category_l3": chunk.rename(columns={t_cat3: "category_l3"}, inplace=True)

    # Transaction id
    if t_txid and t_txid in chunk.columns:
        chunk["transaction_id"] = chunk[t_txid].apply(ensure_uuid)
    else:
        chunk["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(chunk))]

    # Timestamp
    if t_ts and t_ts in chunk.columns:
        # Try to parse; replace NaT with synthetic
        parsed = pd.to_datetime(chunk[t_ts], errors="coerce", utc=True)
        chunk["timestamp"] = [ (pd_ts.isoformat().replace("+00:00","Z") if pd.notna(pd_ts) else synth_time())
                               for pd_ts in parsed ]
    else:
        chunk["timestamp"] = [synth_time() for _ in range(len(chunk))]

    # Keep only canonical columns in final shape
    keep_cols = [
        "transaction_id","timestamp","user_id",
        "first_name","last_name","email","address",
        "category_l1","category_l2","category_l3",
        "quantity","unit_price","total_price",
    ]
    tx_rows.append(chunk[keep_cols])

# Concatenate and cap to TARGET_TX
tx_expanded = (pd.concat(tx_rows, ignore_index=True) if tx_rows else pd.DataFrame(columns=[
    "transaction_id","timestamp","user_id","first_name","last_name","email","address",
    "category_l1","category_l2","category_l3","quantity","unit_price","total_price"
])).iloc[:TARGET_TX].copy()

# Final tidy: types/strings
tx_expanded["transaction_id"] = tx_expanded["transaction_id"].apply(ensure_uuid)
tx_expanded["timestamp"] = tx_expanded["timestamp"].apply(lambda s: (s or synth_time()))
tx_expanded["quantity"] = tx_expanded["quantity"].apply(lambda q: int(max(1, safe_int(q, 1))))
tx_expanded["unit_price"] = tx_expanded["unit_price"].apply(lambda p: round(safe_float(p, 1.0), 2))
tx_expanded["total_price"] = (tx_expanded["unit_price"].astype(float) * tx_expanded["quantity"].astype(int)).round(2)

# ------------- save -------------
OUT_CUSTOMERS.parent.mkdir(parents=True, exist_ok=True)
cust_expanded.to_csv(OUT_CUSTOMERS, index=False)
tx_expanded.to_csv(OUT_TX, index=False)

print(f"Wrote {len(cust_expanded):,} customers -> {OUT_CUSTOMERS}")
print(f"Wrote {len(tx_expanded):,} transactions -> {OUT_TX}")