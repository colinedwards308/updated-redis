# backend/main.py
from __future__ import annotations

# FastAPI
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .db_bootstrap import ensure_schema

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

@app.on_event("startup")
def boot():
    ensure_schema()

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
import os, socket
from urllib.parse import urlsplit
from fastapi import FastAPI

app = FastAPI()

@app.get("/api/redis-stats")
def redis_stats():
    r = redis_client()
    info = r.info()

    # --- keyspace count ---
    db_keys = 0
    db_display = None

    url = settings.REDIS_URL.strip()
    parts = urlsplit(url)

    db_str = (parts.path or "/0").lstrip("/")
    try:
        db_idx = int(db_str) if db_str else 0
    except ValueError:
        db_idx = 0

    # Detect Enterprise/Cluster
    if info.get("cluster_enabled") == 1:
        # Enterprise/Cluster always uses DB 0 internally, but SELECT is disabled
        db_display = "ENTERPRISE"
        db_section = info.get("db0", {})
        db_keys = db_section.get("keys", 0) if isinstance(db_section, dict) else 0
    else:
        db_display = str(db_idx)
        db_section = info.get(f"db{db_idx}", {})
        db_keys = db_section.get("keys", 0) if isinstance(db_section, dict) else 0

    try:
        resolved_ip = socket.gethostbyname(parts.hostname or "localhost")
    except Exception:
        resolved_ip = None

    display_url = url
    if parts.password:
        display_url = display_url.replace(f":{parts.password}@", ":********@")

    return {
        "success": True,
        "connection": {
            "redis_url": display_url,
            "host": parts.hostname or "localhost",
            "port": parts.port or 6379,
            "db": db_display,     # <-- returns "0" or "ENTERPRISE"
            "resolved_ip": resolved_ip,
        },
        "redis_stats": {
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "total_commands_processed": info.get("total_commands_processed", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            "db_keys": db_keys,
        },
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
    # -------------- HEAVY SUMMARY --------------
    summary_q = text("""
        WITH tx_f AS (
            SELECT
                t.user_id,
                t.total_price::float AS total_price,
                t.quantity::int      AS qty,
                t.category_l1,
                t.category_l2,
                t.category_l3,
                t.timestamp,
                date_trunc('day',  t.timestamp) AS d_day,
                date_trunc('week', t.timestamp) AS d_week
            FROM transactions t
            WHERE (CAST(:since_days AS INT) <= 0
                   OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        ),
        per_user AS (
            SELECT
                user_id,
                COUNT(*)                         AS tx_count,
                SUM(total_price)                 AS user_total,
                SUM(qty)                         AS items_total,
                COUNT(DISTINCT category_l1)      AS categories_distinct,
                MIN(timestamp)                   AS first_purchase,
                MAX(timestamp)                   AS last_purchase
            FROM tx_f
            GROUP BY user_id
        ),
        per_user_ranked AS (
            SELECT
                user_id,
                tx_count,
                user_total,
                items_total,
                categories_distinct,
                first_purchase,
                last_purchase,
                RANK() OVER (ORDER BY user_total DESC) AS spend_rank
            FROM per_user
        ),
        cat_rollup AS (
            -- rollup across category levels (l1, l2, l3)
            SELECT
                COALESCE(category_l1, 'ALL') AS category_l1,
                COALESCE(category_l2, '—')   AS category_l2,
                COALESCE(category_l3, '—')   AS category_l3,
                SUM(total_price)             AS revenue,
                COUNT(*)                     AS tx_count
            FROM tx_f
            GROUP BY ROLLUP (category_l1, category_l2, category_l3)
        ),
        ts_daily AS (
            -- time series aggregation (daily)
            SELECT
                d_day,
                SUM(total_price) AS revenue,
                COUNT(*)         AS tx_count
            FROM tx_f
            GROUP BY d_day
        ),
        percentiles AS (
            -- user_total distribution percentiles
            SELECT
                percentile_cont(0.50) WITHIN GROUP (ORDER BY user_total) AS p50,
                percentile_cont(0.90) WITHIN GROUP (ORDER BY user_total) AS p90,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY user_total) AS p99
            FROM per_user
        ),
        category_top AS (
            -- top category by revenue
            SELECT category_l1, SUM(total_price) AS category_revenue
            FROM tx_f
            GROUP BY category_l1
            ORDER BY category_revenue DESC
            LIMIT 1
        )
        SELECT
            (SELECT COUNT(*) FROM per_user)                                 AS total_active_shoppers,
            COALESCE( (SELECT SUM(user_total) FROM per_user), 0 )           AS total_cart_value,
            COALESCE( (SELECT AVG(user_total) FROM per_user), 0 )           AS average_cart_value,
            COALESCE( (SELECT AVG(tx_count) FROM per_user), 0 )             AS avg_transactions_per_customer,
            COALESCE( (SELECT AVG(categories_distinct) FROM per_user), 0 )  AS avg_categories_per_customer,
            COALESCE( (SELECT COUNT(*) FROM per_user WHERE user_total > 100), 0 ) AS high_value_customers,
            COALESCE( (SELECT COUNT(*) FROM per_user WHERE tx_count >= 10), 0 )  AS frequent_customers,
            COALESCE( (SELECT COUNT(DISTINCT category_l1) FROM tx_f), 0 )   AS total_categories,
            (SELECT category_l1 FROM category_top)                          AS top_category,
            (SELECT category_revenue FROM category_top)                     AS top_category_revenue,
            COALESCE( (SELECT p50 FROM percentiles), 0 )                    AS p50_user_total,
            COALESCE( (SELECT p90 FROM percentiles), 0 )                    AS p90_user_total,
            COALESCE( (SELECT p99 FROM percentiles), 0 )                    AS p99_user_total
        ;
    """)
    summary = session.execute(summary_q, {"since_days": since_days}).mappings().first() or {}

    # -------------- HEAVY TOP CLIENTS --------------
    top_clients_q = text("""
        WITH tx_f AS (
            SELECT
                t.user_id,
                t.total_price::float AS total_price,
                t.timestamp
            FROM transactions t
            WHERE (CAST(:since_days AS INT) <= 0
                   OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        ),
        per_user AS (
            SELECT
                user_id,
                SUM(total_price) AS user_total,
                COUNT(*)         AS tx_count,
                MAX(timestamp)   AS last_purchase
            FROM tx_f
            GROUP BY user_id
        )
        SELECT
            c.id::text                AS user_id,
            (COALESCE(NULLIF(TRIM(c.first_name), ''), '') || ' ' ||
             COALESCE(NULLIF(TRIM(c.last_name),  ''), ''))    AS name,
            COALESCE(c.email, '')                              AS email,
            pu.user_total::float                               AS total_spent,
            pu.tx_count::int                                   AS total_purchases,
            pu.last_purchase                                   AS last_purchase
        FROM per_user pu
        JOIN customers c ON c.id = pu.user_id
        ORDER BY pu.user_total DESC
        LIMIT :limit
    """)
    top_clients = session.execute(top_clients_q, {"since_days": since_days, "limit": limit}).mappings().all()

    # -------------- HEAVY PER-CLIENT CART COMPOSITION --------------
    # Still heavy but targeted: aggregate per top client with item breakdown
    item_q = text("""
        SELECT
            COALESCE(NULLIF(TRIM(category_l3), ''), 
                     NULLIF(TRIM(category_l2), ''), 
                     NULLIF(TRIM(category_l1), '')) AS name,
            COALESCE(NULLIF(TRIM(category_l2), ''), category_l1) AS category,
            SUM(quantity)::int                              AS quantity,
            AVG(unit_price)::float                          AS price
        FROM transactions
        WHERE user_id = :uid
          AND (CAST(:since_days AS INT) <= 0
               OR timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        GROUP BY 1, 2
        ORDER BY quantity DESC
        LIMIT 50
    """)

    carts = []
    for tc in top_clients:
        rows_raw = session.execute(item_q, {"uid": tc["user_id"], "since_days": since_days}).mappings().all()
        items = [
            {
                "name": (r.get("name") or ""),
                "category": (r.get("category") or ""),
                "quantity": int(r.get("quantity") or 0),
                "price": float(r.get("price") or 0.0),
            }
            for r in rows_raw
        ]
        cart_value = float(sum(i["price"] * i["quantity"] for i in items))
        carts.append({
            "client_name": tc["name"] or "",
            "cart_value": round(cart_value, 2),
            "items_count": sum(i["quantity"] for i in items),
            "items": items,
        })

    # -------------- SHAPE JSON (types are JSON-safe) --------------
    return {
        "summary": {
            "total_active_shoppers": int(summary.get("total_active_shoppers") or 0),
            "total_cart_value": float(summary.get("total_cart_value") or 0.0),
            "average_cart_value": float(summary.get("average_cart_value") or 0.0),
            "avg_transactions_per_customer": float(summary.get("avg_transactions_per_customer") or 0.0),
            "avg_categories_per_customer": float(summary.get("avg_categories_per_customer") or 0.0),
            "high_value_customers": int(summary.get("high_value_customers") or 0),
            "frequent_customers": int(summary.get("frequent_customers") or 0),
            "total_categories": int(summary.get("total_categories") or 0),
            "top_category": (summary.get("top_category") or ""),
            "top_category_revenue": float(summary.get("top_category_revenue") or 0.0),
            "p50_user_total": float(summary.get("p50_user_total") or 0.0),
            "p90_user_total": float(summary.get("p90_user_total") or 0.0),
            "p99_user_total": float(summary.get("p99_user_total") or 0.0),
        },
        "top_clients": [
            {
                "user_id": str(tc["user_id"]),  # UUID -> string
                "name": tc["name"] or "",
                "email": tc["email"] or "",
                "total_spent": float(tc["total_spent"] or 0.0),
                "total_purchases": int(tc["total_purchases"] or 0),
                "last_purchase": (tc["last_purchase"].isoformat() if tc.get("last_purchase") else None),
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
    return _wrap({"report": report}, started, cached=False, ttl=None)

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
        WITH tx_f AS (
            SELECT
                t.user_id,
                t.quantity::int      AS qty,
                t.total_price::float AS total_price,
                t.category_l1,
                t.timestamp,
                date_trunc('week', t.timestamp) AS d_week
            FROM transactions t
            WHERE (CAST(:since_days AS INT) <= 0
                   OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        ),
        per_user AS (
            SELECT
                user_id,
                SUM(qty)                  AS items_total,
                SUM(total_price)          AS spend_total,
                COUNT(*)                  AS tx_count,
                COUNT(DISTINCT category_l1) AS cats,
                MAX(timestamp)            AS last_active
            FROM tx_f
            GROUP BY user_id
        ),
        recency_buckets AS (
            SELECT
                user_id,
                last_active,
                CASE
                    WHEN last_active >= NOW() - interval '1 day'  THEN '24h'
                    WHEN last_active >= NOW() - interval '7 days' THEN '7d'
                    ELSE 'older'
                END AS recency
            FROM per_user
        ),
        ranks AS (
            SELECT
                pu.user_id,
                pu.items_total,
                pu.spend_total,
                pu.tx_count,
                pu.cats,
                pu.last_active,
                rb.recency,
                RANK()       OVER (ORDER BY pu.spend_total DESC)   AS r_spend,
                ROW_NUMBER() OVER (ORDER BY pu.last_active DESC)   AS r_recent
            FROM per_user pu
            JOIN recency_buckets rb USING (user_id)
        )
        SELECT
            c.id AS user_id,
            (COALESCE(NULLIF(TRIM(c.first_name), ''), '') || ' ' ||
             COALESCE(NULLIF(TRIM(c.last_name),  ''), '')) AS name,
            COALESCE(c.email, '')                           AS email,
            ranks.items_total::int                          AS cart_items_count,
            ranks.spend_total::float                        AS cart_value,
            ranks.last_active                               AS last_active
        FROM ranks
        JOIN customers c ON c.id = ranks.user_id
        ORDER BY ranks.spend_total DESC, ranks.last_active DESC
    """
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
            "user_id": str(rr["user_id"]),  # <— make JSON-safe
            "name": rr["name"] or "",
            "email": rr["email"] or "",
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
                "user_id": str(rr["user_id"]),  # <— make JSON-safe
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
        WITH tx_f AS (
            SELECT
                t.user_id,
                -- Resolve a human-friendly item name & category
                COALESCE(NULLIF(TRIM(t.category_l3), ''), 
                         NULLIF(TRIM(t.category_l2), ''), 
                         NULLIF(TRIM(t.category_l1), ''))      AS name,
                COALESCE(NULLIF(TRIM(t.category_l2), ''), t.category_l1) AS category,
                t.quantity::int      AS qty,
                t.unit_price::float  AS unit_price,
                t.total_price::float AS total_price,
                t.timestamp,
                date_trunc('week', t.timestamp) AS d_week
            FROM transactions t
            WHERE (CAST(:since_days AS INT) <= 0
                   OR t.timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
        ),
        per_item AS (
            SELECT
                name,
                category,
                SUM(qty)                           AS purchase_count,
                AVG(unit_price)                    AS avg_unit_price,
                SUM(total_price)                   AS revenue,
                COUNT(*)                           AS tx_count,
                COUNT(DISTINCT user_id)            AS distinct_buyers,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_price) AS p50_price
            FROM tx_f
            WHERE name IS NOT NULL
            GROUP BY name, category
        ),
        trend AS (
            SELECT
                name, category,
                SUM(CASE WHEN d_week >= date_trunc('week', NOW()) - interval '7 days'
                         THEN qty ELSE 0 END)      AS qty_this_week,
                SUM(CASE WHEN d_week <  date_trunc('week', NOW()) - interval '7 days'
                          AND d_week >= date_trunc('week', NOW()) - interval '14 days'
                         THEN qty ELSE 0 END)      AS qty_prev_week
            FROM tx_f
            WHERE name IS NOT NULL
            GROUP BY name, category
        ),
        ranked AS (
            SELECT
                p.name,
                p.category,
                p.purchase_count,
                p.avg_unit_price,
                p.revenue,
                p.distinct_buyers,
                t.qty_this_week,
                t.qty_prev_week,
                RANK() OVER (ORDER BY p.purchase_count DESC) AS r_by_qty,
                RANK() OVER (ORDER BY p.revenue DESC)        AS r_by_rev
            FROM per_item p
            LEFT JOIN trend t
              ON t.name = p.name AND t.category = p.category
        )
        SELECT
            name,
            category,
            avg_unit_price::float AS price,
            purchase_count::int   AS purchase_count
        FROM ranked
        WHERE name IS NOT NULL
        ORDER BY purchase_count DESC, revenue DESC
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
    return _wrap({"success": True, "popular_items": items}, started, cached=False, ttl=None)

# -------------------------------------------------------
# 5) Customer drill-down  (JSON-safe)
# -------------------------------------------------------
from uuid import UUID
from fastapi import HTTPException

@app.get("/api/customers/{user_id}")
def customer_detail(user_id: str, since_days: int | None = None, db: Session = Depends(get_db)):
    try:
        uid = UUID(user_id)          # validate & convert once
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user_id")

    started = perf_counter()
    rc = redis_client()
    k = key("customer", f"{user_id}:{since_days if since_days is not None else 'all'}")

    cached = get_json(rc, k)
    if cached is not None:
        return _wrap(cached, started, cached=True, ttl=rc.ttl(k))

    with db as session:
        row = session.execute(text("""
            SELECT id::text AS id, first_name, last_name, email
            FROM customers
            WHERE id = :id                -- no cast needed
        """), {"id": uid}).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Customer not found")

        customer = {
            "id": row["id"],  # already text
            "name": f"{(row['first_name'] or '').strip()} {(row['last_name'] or '').strip()}".strip(),
            "email": row["email"] or "",
        }

        tx_rows = session.execute(text("""
            SELECT
                id::text AS id,
                timestamp,
                COALESCE(NULLIF(TRIM(category_l3), ''), 
                         NULLIF(TRIM(category_l2), ''), 
                         NULLIF(TRIM(category_l1), '')) AS item,
                quantity,
                unit_price::float   AS unit_price,
                total_price::float  AS total_price
            FROM transactions
            WHERE user_id = :uid          -- no cast needed
              AND (CAST(:since_days AS INT) <= 0 OR timestamp >= NOW() - make_interval(days => CAST(:since_days AS INT)))
            ORDER BY timestamp DESC
        """), {"uid": uid, "since_days": since_days or 0}).mappings().all()

        transactions = []
        for t in tx_rows:
            ts = t.get("timestamp")
            transactions.append({
                "id": t.get("id"),                    # already text
                "timestamp": ts.isoformat() if ts else None,
                "item": t.get("item") or "",
                "quantity": int(t.get("quantity") or 0),
                "unit_price": float(t.get("unit_price") or 0.0),
                "total_price": float(t.get("total_price") or 0.0),
            })

        detail = {"customer": customer, "transactions": transactions}

    set_json(rc, k, detail, ttl=None)
    return _wrap(detail, started, cached=False, ttl=None)

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