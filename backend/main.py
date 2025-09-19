# backend/main.py
from __future__ import annotations

# FastAPI
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Stdlib
from time import perf_counter
import math
import os
import pathlib

# SQLAlchemy
from sqlalchemy.orm import Session
from sqlalchemy import text

# Internal modules
from .db import SessionLocal
from .settings import settings
from .cache import redis_client, key, get_json, set_json, delete_prefix
from .seed import create_schema, load_from_csv, wipe_all_data

# -------------------------------------------------------
# App setup
# -------------------------------------------------------
app = FastAPI(title="Redis Retail Demo (CSV-backed)", version="1.0.0")

# Static UI (only / and /static, no catch-all)
PUBLIC_DIR = pathlib.Path(__file__).resolve().parents[1] / "public"
if PUBLIC_DIR.is_dir():
    # If you later add css/js files, serve them from /static
    app.mount("/static", StaticFiles(directory=str(PUBLIC_DIR), html=False), name="static")

    @app.get("/", include_in_schema=False)
    async def root_index():
        index_path = PUBLIC_DIR / "index.html"
        if not index_path.is_file():
            raise HTTPException(status_code=404, detail="index.html not found")
        return FileResponse(index_path)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _ms(start: float) -> int:
    return math.ceil((perf_counter() - start) * 1000)

def _wrap(payload: dict, started: float, *, cached: bool | None = None, ttl: int | None = None) -> dict:
    payload.setdefault("elapsed_ms", _ms(started))
    if cached is not None:
        payload["cached"] = cached
    payload["ttl_seconds"] = ttl if ttl is not None else None
    return payload

@app.on_event("startup")
def _startup():
    create_schema()

# -------------------------------------------------------
# 0) Redis stats  (UI calls /api/redis-stats)
# -------------------------------------------------------
@app.get("/api/redis-stats")
def redis_stats():
    r = redis_client()
    info = r.info()
    db_keys = 0
    if "db0" in info and isinstance(info["db0"], dict):
        db_keys = info["db0"].get("keys", 0)
    return {
        "success": True,
        "redis_stats": {
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "total_commands_processed": info.get("total_commands_processed", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            "db_keys": db_keys,
        }
    }

# -------------------------------------------------------
# 1) Load Sample Data (CSV)  (UI posts to /api/redis/load-sample-data)
# -------------------------------------------------------
@app.post("/api/redis/load-sample-data")
def load_sample_data(payload: dict | None = None):
    body = payload or {}
    in_customers = body.get("customers_csv") or "datafiles/customers_clean.csv"
    in_tx        = body.get("transactions_csv") or "datafiles/transactions_clean.csv"
    reset        = bool(body.get("reset", True))

    # resolve relative to project root (../public/.. -> project root is one up from backend/)
    def _resolve(p: str) -> str:
        pth = pathlib.Path(p)
        if pth.is_file():
            return str(pth)
        # try relative to project root
        guess = PUBLIC_DIR.parent / p if PUBLIC_DIR.is_dir() else pathlib.Path(__file__).resolve().parents[1] / p
        return str(guess)

    customers_csv    = _resolve(in_customers)
    transactions_csv = _resolve(in_tx)

    stats = load_from_csv(customers_csv, transactions_csv, reset=reset)

    # clear demo caches
    r = redis_client()
    delete_prefix(r, key("report", ""))           # demo:report:*
    delete_prefix(r, key("active_shoppers", ""))  # demo:active_shoppers:*
    delete_prefix(r, key("customer", ""))         # demo:customer:*
    delete_prefix(r, key("popular", ""))          # demo:popular:*

    headline = {
        "customers_loaded": stats.get("customers_loaded", 0),
        "transactions_loaded": stats.get("transactions_loaded", 0),
        "clients_loaded": stats.get("customers_loaded", 0),   # for older UI copy
        "products_loaded": stats.get("products_loaded", 39),  # friendly placeholder
        "active_shoppers": stats.get("active_shoppers", 0),
    }
    return {
        "success": True,
        "message": f"CSV load complete. Loaded {headline['products_loaded']} products, {headline['clients_loaded']} clients, {headline['active_shoppers']} active shoppers.",
        "stats": headline
    }

# -------------------------------------------------------
# 2) Retail report (uncached & cached)
# -------------------------------------------------------
def _build_retail_report(session: Session, since_days: int = 0, limit: int = 10) -> dict:
    # 1) Summary
    summary_q = text("""
        SELECT
            COUNT(DISTINCT t.user_id)       AS total_active_shoppers,
            COALESCE(SUM(t.total_price),0)  AS total_cart_value,
            COALESCE(AVG(t.total_price),0)  AS average_cart_value
        FROM transactions t
        WHERE (CAST(:since_days AS INT) <= 0 OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
    """)
    summary = session.execute(summary_q, {"since_days": since_days}).mappings().first() or {}

    # 2) Top clients by spend
    top_q = text("""
        SELECT
            c.id AS user_id,
            (COALESCE(NULLIF(TRIM(c.first_name), ''), '') || ' ' || COALESCE(NULLIF(TRIM(c.last_name), ''), '')) AS name,
            COALESCE(c.email, '') AS email,
            ROUND(SUM(t.total_price)::numeric, 2) AS total_spent,
            COUNT(*) AS total_purchases
        FROM customers c
        JOIN transactions t ON t.user_id = c.id
        WHERE (CAST(:since_days AS INT) <= 0 OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        GROUP BY c.id, c.first_name, c.last_name, c.email
        ORDER BY total_spent DESC
        LIMIT :limit
    """)
    top_clients = session.execute(top_q, {"since_days": since_days, "limit": limit}).mappings().all()

    # 3) Cart details per top client
    item_q = text("""
        SELECT
            COALESCE(NULLIF(TRIM(category_l3), ''), 
                     NULLIF(TRIM(category_l2), ''), 
                     NULLIF(TRIM(category_l1), '')) AS name,
            COALESCE(NULLIF(TRIM(category_l2), ''), category_l1) AS category,
            SUM(quantity) AS quantity,
            AVG(unit_price)::float AS price
        FROM transactions
        WHERE user_id = :uid
          AND (CAST(:since_days AS INT) <= 0 OR timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        GROUP BY 1, 2
        ORDER BY quantity DESC
        LIMIT 20
    """)
    carts = []
    for tc in top_clients:
        rows_raw = session.execute(item_q, {"uid": tc["user_id"], "since_days": since_days}).mappings().all()

        # materialize to JSON-safe dicts
        items = [
            {
                "name": (row.get("name") or ""),
                "category": (row.get("category") or ""),
                "quantity": int(row.get("quantity") or 0),
                "price": float(row.get("price") or 0.0),
            }
            for row in rows_raw
        ]

        cart_value = float(sum(i["price"] * i["quantity"] for i in items))
        carts.append({
            "client_name": tc["name"],
            "cart_value": round(cart_value, 2),
            "items_count": sum(i["quantity"] for i in items),
            "items": items,
        })

    # 4) Final shape
    return {
        "summary": {
            "total_active_shoppers": int(summary.get("total_active_shoppers", 0)),
            "total_cart_value": float(summary.get("total_cart_value", 0.0)),
            "average_cart_value": float(summary.get("average_cart_value", 0.0)),
        },
        "top_clients": [
            {
                "user_id": tc["user_id"],
                "name": tc["name"],
                "email": tc["email"],
                "total_spent": float(tc["total_spent"] or 0.0),
                "total_purchases": int(tc["total_purchases"] or 0),
            }
            for tc in top_clients
        ],
        "shopping_carts": carts,
    }

@app.get("/api/retail-report")
def retail_report_main(since_days: int = 30, limit: int = 10, db: Session = Depends(get_db)):
    started = perf_counter()
    with db as session:
        report = _build_retail_report(session, since_days=since_days, limit=limit)
    return _wrap({"report": report}, started, cached=False, ttl=None)

@app.get("/api/retail-report-cached")
def retail_report_cached(since_days: int = 30, limit: int = 10, db: Session = Depends(get_db)):
    started = perf_counter()
    rc = redis_client()  # clearer name
    k = key("report", f"retail:{since_days}:{limit}")

    cached = get_json(rc, k)
    if cached is not None:
        return _wrap({"report": cached}, started, cached=True, ttl=rc.ttl(k))

    with db as session:
        report = _build_retail_report(session, since_days=since_days, limit=limit)

    set_json(rc, k, report, ttl=60)
    return _wrap({"report": report}, started, cached=False, ttl=60)

# -------------------------------------------------------
# 3) Active shoppers (uncached & cached)
# -------------------------------------------------------
def _query_active_shoppers(session: Session, since_days: int = 30, limit: int | None = None):
    # Normalize / clamp limit (0 or negative -> no limit; positive -> min(limit, 1200))
    if isinstance(limit, int) and limit > 0:
        limit = min(limit, 1200)
    else:
        limit = None

    base_sql = """
        SELECT
            c.id AS user_id,
            (COALESCE(NULLIF(TRIM(c.first_name), ''), '') || ' ' || COALESCE(NULLIF(TRIM(c.last_name), ''), '')) AS name,
            COALESCE(c.email, '') AS email,
            SUM(t.quantity)::int      AS cart_items_count,
            SUM(t.total_price)::float AS cart_value,
            MAX(t.timestamp)          AS last_active
        FROM customers c
        JOIN transactions t ON t.user_id = c.id
        WHERE (CAST(:since_days AS INT) <= 0 OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        GROUP BY c.id, c.first_name, c.last_name, c.email
        ORDER BY cart_value DESC
    """

    # Append LIMIT only when we actually have one
    if limit is not None:
        base_sql += "\nLIMIT :limit"

    sql = text(base_sql)
    params = {"since_days": since_days}
    if limit is not None:
        params["limit"] = limit

    rows = session.execute(sql, params).mappings().all()
    return rows

# --- Cached endpoint with cap (max 1200) ---
@app.get("/api/active-shoppers-cached")
def active_shoppers_cached(since_days: int = 30, limit: int = 1200, db: Session = Depends(get_db)):
    # Clamp here too so cache keys are consistent with actual query
    lim = min(limit, 1200) if (isinstance(limit, int) and limit > 0) else None

    started = perf_counter()
    r = redis_client()
    k = key("active_shoppers", f"{since_days}:{lim if lim is not None else 'all'}")

    cached = get_json(r, k)
    if cached is not None:
        return _wrap({"total_count": len(cached), "active_shoppers": cached}, started, cached=True, ttl=r.ttl(k))

    with db as session:
        rows = _query_active_shoppers(session, since_days=since_days, limit=lim)
        # Materialize to JSON-safe dicts
        rows = [
            {
                "user_id": rr["user_id"],
                "name": rr["name"],
                "email": rr["email"],
                "cart_items_count": int(rr["cart_items_count"] or 0),
                "cart_value": float(rr["cart_value"] or 0.0),
                "last_active": rr["last_active"].isoformat() if rr["last_active"] else None,
            }
            for rr in rows
        ]

    set_json(r, k, rows, ttl=60)
    return _wrap({"total_count": len(rows), "active_shoppers": rows}, started, cached=False, ttl=60)

# --- Compatibility alias: legacy /api/active-shoppers -> return cached data ---
@app.get("/api/active-shoppers")
def active_shoppers_main(since_days: int = 30, limit: int = 1200, db: Session = Depends(get_db)):
    # Clamp limit to max 1200; ignore non-positive values (treat as no limit)
    lim = min(limit, 1200) if (isinstance(limit, int) and limit > 0) else None

    started = perf_counter()
    with db as session:
        rows = _query_active_shoppers(session, since_days=since_days, limit=lim)
        # materialize to JSON-safe dicts (same shape as cached endpoint)
        rows = [
            {
                "user_id": rr["user_id"],
                "name": rr["name"],
                "email": rr["email"],
                "cart_items_count": int(rr["cart_items_count"] or 0),
                "cart_value": float(rr["cart_value"] or 0.0),
                "last_active": rr["last_active"].isoformat() if rr["last_active"] else None,
            }
            for rr in rows
        ]

    return _wrap({"total_count": len(rows), "active_shoppers": rows}, started, cached=False, ttl=None)

# -------------------------------------------------------
# 4) Popular items (uncached & cached)
# -------------------------------------------------------
def _query_popular_items(session: Session, since_days: int = 30, limit: int = 10):
    sql = text("""
        SELECT
            COALESCE(NULLIF(TRIM(category_l3), ''), 
                     NULLIF(TRIM(category_l2), ''), 
                     NULLIF(TRIM(category_l1), ''))                      AS name,
            COALESCE(NULLIF(TRIM(category_l2), ''), category_l1)         AS category,
            AVG(unit_price)::float                                       AS price,
            SUM(quantity)::int                                           AS purchase_count  -- cast to INT
        FROM transactions
        WHERE (CAST(:since_days AS INT) <= 0 OR timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        GROUP BY 1, 2
        HAVING COALESCE(NULLIF(TRIM(category_l3), ''), 
                        NULLIF(TRIM(category_l2), ''), 
                        NULLIF(TRIM(category_l1), '')) IS NOT NULL
        ORDER BY purchase_count DESC
        LIMIT :limit
    """)
    return session.execute(sql, {"since_days": since_days, "limit": limit}).mappings().all()

@app.get("/api/popular-items")
def popular_items_main(since_days: int = 30, limit: int = 10, db: Session = Depends(get_db)):
    started = perf_counter()
    with db as session:
        items = _query_popular_items(session, since_days=since_days, limit=limit)
        # ensure plain dicts and numeric types
        items = [
            {
                "name": r["name"],
                "category": r["category"],
                "price": float(r["price"] or 0.0),
                "purchase_count": int(r["purchase_count"] or 0),
            }
            for r in items
        ]
    return _wrap({"success": True, "popular_items": items}, started, cached=False, ttl=None)

@app.get("/api/popular-items-cached")
def popular_items_cached(since_days: int = 30, limit: int = 10, db: Session = Depends(get_db)):
    started = perf_counter()
    rc = redis_client()
    k = key("popular", f"{since_days}:{limit}")
    cached = get_json(rc, k)
    if cached is not None:
        return _wrap({"success": True, "popular_items": cached}, started, cached=True, ttl=rc.ttl(k))

    with db as session:
        items = _query_popular_items(session, since_days=since_days, limit=limit)
        # materialize to JSON-safe dicts **before** caching
        items = [
            {
                "name": r["name"],
                "category": r["category"],
                "price": float(r["price"] or 0.0),
                "purchase_count": int(r["purchase_count"] or 0),
            }
            for r in items
        ]

    set_json(rc, k, items, ttl=60)
    return _wrap({"success": True, "popular_items": items}, started, cached=False, ttl=60)

# -------------------------------------------------------
# 5) Customer drill-down
# -------------------------------------------------------
# -------------------------------------------------------
# 5) Customer drill-down  (JSON-safe)
# -------------------------------------------------------
@app.get("/api/customers/{user_id}")
def customer_detail(user_id: str, since_days: int | None = None, db: Session = Depends(get_db)):
    started = perf_counter()
    rc = redis_client()
    k = key("customer", f"{user_id}:{since_days if since_days is not None else 'all'}")

    cached = get_json(rc, k)
    if cached is not None:
        return _wrap(cached, started, cached=True, ttl=rc.ttl(k))

    with db as session:
        # Fetch customer
        row = session.execute(
            text("SELECT id, first_name, last_name, email FROM customers WHERE id = :id"),
            {"id": user_id},
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Customer not found")

        customer = {
            "id": row["id"],
            "name": f"{(row['first_name'] or '').strip()} {(row['last_name'] or '').strip()}".strip(),
            "email": row["email"] or "",
        }

        # Fetch their transactions (materialize to JSON-safe)
        tx_rows = session.execute(text("""
            SELECT
                id,
                timestamp,
                COALESCE(NULLIF(TRIM(category_l3), ''), 
                         NULLIF(TRIM(category_l2), ''), 
                         NULLIF(TRIM(category_l1), '')) AS item,
                quantity,
                unit_price::float   AS unit_price,
                total_price::float  AS total_price
            FROM transactions
            WHERE user_id = :uid
              AND (CAST(:since_days AS INT) <= 0 OR timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
            ORDER BY timestamp DESC
        """), {"uid": user_id, "since_days": since_days if since_days is not None else 0}).mappings().all()

        transactions = []
        for t in tx_rows:
            ts = t.get("timestamp")
            transactions.append({
                "id": t.get("id"),
                "timestamp": ts.isoformat() if ts else None,  # <- JSON-safe
                "item": t.get("item") or "",
                "quantity": int(t.get("quantity") or 0),
                "unit_price": float(t.get("unit_price") or 0.0),
                "total_price": float(t.get("total_price") or 0.0),
            })

        detail = {"customer": customer, "transactions": transactions}

    # Cache JSON-safe structure
    set_json(rc, k, detail, ttl=60)
    return _wrap(detail, started, cached=False, ttl=60)

# -------------------------------------------------------
# 6) Clear data (DB + Redis)
# -------------------------------------------------------
@app.post("/api/clear-data")
def clear_data():
    started = perf_counter()
    wipe_all_data()

    r = redis_client()
    removed = delete_prefix(r, settings.CACHE_PREFIX + ":")

    with SessionLocal() as session:
        customers = session.execute(text("SELECT COUNT(*) FROM customers")).scalar() or 0
        tx        = session.execute(text("SELECT COUNT(*) FROM transactions")).scalar() or 0

    return _wrap({
        "success": True,
        "message": f"Database wiped and {removed} Redis keys cleared.",
        "stats": {
            "customers_loaded": customers,
            "transactions_loaded": tx,
            "active_shoppers": 0,
        }
    }, started, cached=False, ttl=None)