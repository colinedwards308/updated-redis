# backend/queries.py
from __future__ import annotations

from typing import Any, List, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text


def _days_filter_sql(col: str) -> str:
    # Enables "all time" when since_days is NULL
    # Postgres: NOW() - ('30 days')::interval
    return f"(:since_days::INT IS NULL OR {col} >= NOW() - (:since_days || ' days')::interval)"


def q_top_customers(
    session: Session,
    limit: int = 10,
    since_days: Optional[int] = 30,
) -> List[Dict[str, Any]]:
    """
    Top customers by total_spent within the optional time window.
    """
    sql = text(f"""
        SELECT
            c.id                AS user_id,
            c.first_name,
            c.last_name,
            c.email,
            COUNT(t.id)                           AS tx_count,
            COALESCE(SUM(t.quantity), 0)          AS total_items,
            COALESCE(SUM(t.total_price), 0)::FLOAT AS total_spent,
            MIN(t.timestamp)                      AS first_tx,
            MAX(t.timestamp)                      AS last_tx
        FROM customers c
        JOIN transactions t ON t.user_id = c.id
        WHERE {_days_filter_sql("t.timestamp")}
        GROUP BY c.id, c.first_name, c.last_name, c.email
        ORDER BY total_spent DESC
        LIMIT :limit
    """)
    rows = session.execute(sql, {"since_days": since_days, "limit": limit}).mappings().all()
    return [dict(r) for r in rows]


def q_active_shoppers(
    session: Session,
    since_days: Optional[int] = 30,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Shoppers with at least one transaction in the window.
    """
    base_sql = f"""
        SELECT
            c.id                                   AS user_id,
            c.first_name,
            c.last_name,
            c.email,
            COUNT(t.id)                            AS tx_count,
            COALESCE(SUM(t.quantity), 0)           AS total_items,
            COALESCE(SUM(t.total_price), 0)::FLOAT AS total_spent,
            MIN(t.timestamp)                       AS first_tx,
            MAX(t.timestamp)                       AS last_tx
        FROM customers c
        JOIN transactions t ON t.user_id = c.id
        WHERE {_days_filter_sql("t.timestamp")}
        GROUP BY c.id, c.first_name, c.last_name, c.email
        ORDER BY total_spent DESC
    """
    if limit:
        base_sql += "\nLIMIT :limit"
    sql = text(base_sql)

    params = {"since_days": since_days}
    if limit:
        params["limit"] = limit

    rows = session.execute(sql, params).mappings().all()
    return [dict(r) for r in rows]


def q_customer_detail(
    session: Session,
    user_id: str,
    since_days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Return a single customer's profile + their transactions (optionally time-filtered),
    plus summary totals.
    """
    # Customer record
    cust_sql = text("SELECT * FROM customers WHERE id = :uid")
    customer = session.execute(cust_sql, {"uid": user_id}).mappings().first()

    if not customer:
        return {"customer": None, "transactions": [], "summary": {"total_items": 0, "total_spent": 0.0}}

    # Transactions list
    tx_sql = text(f"""
        SELECT
            id,
            timestamp,
            user_id,
            first_name,
            last_name,
            email,
            address,
            category_l1,
            category_l2,
            category_l3,
            quantity,
            unit_price::FLOAT  AS unit_price,
            total_price::FLOAT AS total_price
        FROM transactions
        WHERE user_id = :uid
          AND {_days_filter_sql("timestamp")}
        ORDER BY timestamp DESC
    """)
    tx_rows = session.execute(tx_sql, {"uid": user_id, "since_days": since_days}).mappings().all()
    transactions = [dict(r) for r in tx_rows]

    # Summary
    sum_sql = text(f"""
        SELECT
            COALESCE(SUM(quantity), 0)           AS total_items,
            COALESCE(SUM(total_price), 0)::FLOAT AS total_spent
        FROM transactions
        WHERE user_id = :uid
          AND {_days_filter_sql("timestamp")}
    """)
    summary = session.execute(sum_sql, {"uid": user_id, "since_days": since_days}).mappings().first() or {}

    return {
        "customer": dict(customer),
        "transactions": transactions,
        "summary": {
            "total_items": int(summary.get("total_items", 0) or 0),
            "total_spent": float(summary.get("total_spent", 0.0) or 0.0),
        }
    }


from sqlalchemy import text

def q_popular_items(session, since_days: int = 30, limit: int = 10):
    """
    Top “items” from transactions, deriving item name/category from category_l3/2/1.
    Uses unit_price avg and total quantity as popularity.
    """
    sql = text("""
        SELECT
            COALESCE(NULLIF(TRIM(category_l3), ''), 
                     NULLIF(TRIM(category_l2), ''), 
                     NULLIF(TRIM(category_l1), ''))              AS name,
            COALESCE(NULLIF(TRIM(category_l2), ''), category_l1) AS category,
            AVG(unit_price)::FLOAT                               AS avg_price,
            SUM(quantity)                                        AS purchase_count
        FROM transactions
        WHERE timestamp >= NOW() - (:since_days * INTERVAL '1 day')
        GROUP BY 1, 2
        HAVING COALESCE(NULLIF(TRIM(category_l3), ''), 
                        NULLIF(TRIM(category_l2), ''), 
                        NULLIF(TRIM(category_l1), '')) IS NOT NULL
        ORDER BY purchase_count DESC
        LIMIT :limit
    """)
    rows = session.execute(sql, {"since_days": since_days, "limit": limit}).mappings().all()
    # map to your API shape
    return [
        {
            "name": r["name"],
            "category": r["category"],
            "price": float(r["avg_price"] or 0.0),
            "purchase_count": int(r["purchase_count"] or 0),
        }
        for r in rows
    ]