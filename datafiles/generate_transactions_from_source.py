#!/usr/bin/env python3
"""
Standardize/enrich transactions from a source JSONL into a clean schema with customer + product details.

Defaults:
  source_jsonl: /home/colin/redis-demo/src/datafiles/transactions.jsonl
  customers_csv: /mnt/data/customers_with_id.csv  (optional join on user_id; will synthesize if missing)
Outputs:
  out_csv: transactions.csv
  out_jsonl: transactions.jsonl
  catalog_out: product_catalog.csv

Usage:
  python generate_transactions_from_source.py \
      --source_jsonl /home/colin/redis-demo/src/datafiles/transactions.jsonl \
      --customers_csv /mnt/data/customers_with_id.csv \
      --out_csv /home/colin/redis-demo/src/datafiles/transactions_clean.csv \
      --out_jsonl /home/colin/redis-demo/src/datafiles/transactions_clean.jsonl

Notes:
- If the source already contains some fields (first_name, etc.), they are preserved unless missing.
- If categories are missing, the script attempts to map product_name to a known catalog, or best-effort classify.
"""

import argparse
import os
import sys
import re
import uuid
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_SOURCE = "/home/colin/redis-demo/src/datafiles/transactions.jsonl"
DEFAULT_CUSTOMERS = "/mnt/data/customers_with_id.csv"

def normalize_email_from_names(first, last, user_id):
    first = str(first or "").strip().lower().replace(" ", "")
    last = str(last or "").strip().lower().replace(" ", "")
    uid = (str(user_id) or "").strip().replace(" ", "")
    suffix = (uid[:8] if uid else f"{uuid.uuid4()}")  # stable if user_id present
    return f"{first}.{last}.{suffix}@example.com" if first or last else f"user.{suffix}@example.com"

def normalize_email(email, first=None, last=None, user_id=None):
    email = str(email or "").strip()
    if email.lower() in ("", "empty", "nan", "none"):
        return normalize_email_from_names(first, last, user_id)
    return email

def full_address_from_cols(row):
    addr1 = str(row.get("Primary_Address","") or "").strip()
    city = str(row.get("City","") or "").strip()
    state = str(row.get("State","") or "").strip()
    zip4 = str(row.get("zip4","") or "").strip()
    parts = [p for p in [addr1, city, state] if p and p.lower() != "all others"]
    if zip4 and zip4.lower() != "all others":
        parts.append(zip4)
    return ", ".join(parts) if parts else None

def build_catalog():
    # (category_l1, category_l2, product_name, unit_price)
    catalog = [
        ("SOFT DRINKS","COKE","Coca-Cola Classic 12oz can",1.09),
        ("SOFT DRINKS","COKE","Diet Coke 12oz can",1.09),
        ("SOFT DRINKS","COKE","Coke Zero Sugar 12oz can",1.09),
        ("SOFT DRINKS","DR PEPPER","Dr Pepper 12oz can",1.09),
        ("SOFT DRINKS","DR PEPPER","Diet Dr Pepper 12oz can",1.09),
        ("SOFT DRINKS","PEPSI","Pepsi 12oz can",1.09),
        ("SOFT DRINKS","PEPSI","Diet Pepsi 12oz can",1.09),
        ("SOFT DRINKS","PEPSICO","Mountain Dew 12oz can",1.09),
        ("SOFT DRINKS","PEPSICO","Starry Lemon Lime 12oz can",1.09),
        ("WATER","BOTTLED","Aquafina 16.9oz",1.19),
        ("WATER","BOTTLED","Dasani 16.9oz",1.19),
        ("SPORTS DRINKS","GATORADE","Gatorade Cool Blue 20oz",1.79),
        ("SPORTS DRINKS","BODYARMOR","BodyArmor Fruit Punch 16oz",1.99),
        ("SNACKS","CHIPS","Lay's Classic 8oz",3.99),
        ("SNACKS","CHIPS","Doritos Nacho Cheese 9.25oz",4.29),
        ("SNACKS","CHIPS","Cheetos Crunchy 8.5oz",4.19),
        ("SNACKS","CANDY","Snickers Bar 1.86oz",1.49),
        ("SNACKS","CANDY","M&M's Peanut 3.27oz",1.89),
        ("SNACKS","CANDY","Reese's Peanut Butter Cups 1.5oz",1.49),
        ("GROCERY","CEREAL","Cheerios 18oz",5.49),
        ("GROCERY","CEREAL","Kellogg's Frosted Flakes 19oz",5.79),
        ("GROCERY","PASTA","Barilla Spaghetti 1lb",1.79),
        ("GROCERY","SAUCE","Rao's Marinara 24oz",7.99),
        ("PRODUCE","FRUIT","Bananas (lb)",0.69),
        ("PRODUCE","FRUIT","Honeycrisp Apples (lb)",2.49),
        ("PRODUCE","VEGGIES","Baby Carrots 1lb",1.79),
        ("DAIRY","MILK","Horizon Organic Whole Milk Half Gallon",5.49),
        ("DAIRY","YOGURT","Chobani Greek Yogurt 5.3oz",1.39),
        ("DAIRY","CHEESE","Sargento Sliced Cheddar 8oz",3.99),
        ("BAKERY","BREAD","Wonder Classic White Bread 20oz",3.49),
        ("BAKERY","BAGELS","Thomas' Plain Bagels 6ct",4.29),
        ("HOUSEHOLD","CLEANING","Tide PODS 42ct",13.99),
        ("HOUSEHOLD","DISH","Dawn Dish Soap 19.4oz",3.29),
        ("HOUSEHOLD","PAPER","Bounty Paper Towels 6 Double Rolls",9.99),
        ("PERSONAL CARE","ORAL CARE","Crest 3D White Toothpaste 4.1oz",3.99),
        ("PERSONAL CARE","ORAL CARE","Colgate Total Toothpaste 4.8oz",3.99),
        ("PERSONAL CARE","SHAMPOO","Head & Shoulders Classic Clean 13.5oz",6.99),
        ("COFFEE & TEA","COFFEE","Starbucks Pike Place Roast 12oz",9.99),
        ("COFFEE & TEA","COFFEE","Peet's Major Dickason's Blend 12oz",10.49),
    ]
    return pd.DataFrame(catalog, columns=["category_l1","category_l2","product_name","unit_price"])

def best_effort_classify(product_name):
    """Fallback categorization if no exact catalog match."""
    name = str(product_name or "").lower()
    if any(k in name for k in ["diet coke","coke zero","coca-cola","coke"]):
        return ("SOFT DRINKS","COKE",product_name)
    if "pepper" in name:
        return ("SOFT DRINKS","DR PEPPER",product_name)
    if "pepsi" in name:
        return ("SOFT DRINKS","PEPSI",product_name)
    if "gatorade" in name:
        return ("SPORTS DRINKS","GATORADE",product_name)
    if "bodyarmor" in name:
        return ("SPORTS DRINKS","BODYARMOR",product_name)
    if any(k in name for k in ["aquafina","dasani"]):
        return ("WATER","BOTTLED",product_name)
    if any(k in name for k in ["doritos","cheetos","lay's","lays"]):
        return ("SNACKS","CHIPS",product_name)
    if any(k in name for k in ["snickers","m&m","reese"]):
        return ("SNACKS","CANDY",product_name)
    if "banana" in name or "apple" in name:
        return ("PRODUCE","FRUIT",product_name)
    if "carrot" in name:
        return ("PRODUCE","VEGGIES",product_name)
    if "milk" in name or "yogurt" in name or "cheddar" in name:
        return ("DAIRY","MILK" if "milk" in name else ("YOGURT" if "yogurt" in name else "CHEESE"), product_name)
    if "bread" in name or "bagel" in name:
        return ("BAKERY","BREAD" if "bread" in name else "BAGELS", product_name)
    if "tide" in name or "pods" in name:
        return ("HOUSEHOLD","CLEANING",product_name)
    if "dawn" in name:
        return ("HOUSEHOLD","DISH",product_name)
    if "bounty" in name:
        return ("HOUSEHOLD","PAPER",product_name)
    if "toothpaste" in name or "crest" in name or "colgate" in name:
        return ("PERSONAL CARE","ORAL CARE",product_name)
    if "shampoo" in name or "head & shoulders" in name:
        return ("PERSONAL CARE","SHAMPOO",product_name)
    if "coffee" in name or "starbucks" in name or "peet" in name:
        return ("COFFEE & TEA","COFFEE",product_name)
    # Default bucket
    return ("GROCERY","MISC",product_name)

def standardize_timestamp(ts):
    if pd.isna(ts):
        return None
    if isinstance(ts, (int, float)):
        # Assume epoch seconds
        try:
            return datetime.utcfromtimestamp(ts).isoformat(timespec="seconds")+"Z"
        except Exception:
            return None
    try:
        # Attempt parse common formats
        dt = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source_jsonl", default=DEFAULT_SOURCE, help="Path to source transactions.jsonl")
    ap.add_argument("--customers_csv", default=DEFAULT_CUSTOMERS, help="Optional customers CSV for join on user_id")
    ap.add_argument("--out_csv", default="transactions.csv")
    ap.add_argument("--out_jsonl", default="transactions.jsonl")
    ap.add_argument("--catalog_out", default="product_catalog.csv")
    args = ap.parse_args()

    # Load source transactions
    src = pd.read_json(args.source_jsonl, lines=True)
    # normalize likely column names
    df = src.rename(columns={
        "product": "product_name",
        "productTitle": "product_name",
        "category": "category_l1",
        "order_id": "transaction_id",
        "txn_id": "transaction_id",
        "ts": "timestamp",
        "time": "timestamp",
        "qty": "quantity",
        "price": "unit_price",
        "unitPrice": "unit_price",
        "total": "total_price",
        "userId": "user_id",
        "firstName": "first_name",
        "lastName": "last_name",
        "emailAddress": "email",
        "addr": "address",
    })

    # Try to join in customer details if missing and we have user_id + customers file
    customers_df = None
    if "user_id" in df.columns and args.customers_csv and Path(args.customers_csv).exists():
        customers_raw = pd.read_csv(args.customers_csv)
        customers_df = customers_raw.rename(columns={
            "Person_First_Name":"first_name",
            "Person_Last_Name":"last_name",
            "EM_Email":"email",
        }).copy()
        customers_df["email"] = customers_raw.apply(lambda r: normalize_email(r.get("EM_Email"), r.get("Person_First_Name"), r.get("Person_Last_Name"), r.get("user_id")), axis=1)
        customers_df["address"] = customers_raw.apply(full_address_from_cols, axis=1)
        customers_df = customers_df[["user_id","first_name","last_name","email","address"]].drop_duplicates()

    if customers_df is not None:
        df = df.merge(customers_df, on="user_id", how="left", suffixes=("","_cust"))

        # Prefer values present in source, then from customers file
        for col in ["first_name","last_name","email","address"]:
            src_col = col
            cust_col = f"{col}_cust"
            if cust_col in df.columns:
                df[src_col] = df[src_col].fillna(df[cust_col])
        # Drop helper cols
        drop_cols = [c for c in df.columns if c.endswith("_cust")]
        if drop_cols:
            df = df.drop(columns=drop_cols)

    # Ensure customer fields exist
    for col in ["user_id","first_name","last_name","email","address"]:
        if col not in df.columns:
            df[col] = None

    # Fill email if empty
    df["email"] = df.apply(lambda r: normalize_email(r.get("email"), r.get("first_name"), r.get("last_name"), r.get("user_id")), axis=1)

    # Assign/standardize transaction_id
    if "transaction_id" not in df.columns:
        df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["transaction_id"] = df["transaction_id"].astype(str)

    # Standardize timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].apply(standardize_timestamp)
    else:
        df["timestamp"] = None

    # Quantity defaults
    if "quantity" not in df.columns:
        df["quantity"] = 1
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)

    # Build catalog, write it, and map products/categories/prices
    catalog_df = build_catalog()
    catalog_df.to_csv(args.catalog_out, index=False)

    # If category/product missing, attempt to infer
    if "product_name" not in df.columns:
        df["product_name"] = None

    # Try exact merge on product_name
    merged = df.merge(catalog_df.rename(columns={"product_name":"_cat_product_name"}),
                      left_on="product_name", right_on="_cat_product_name", how="left")

    # Fill categories from catalog where matched
    for col in ["category_l1","category_l2","unit_price"]:
        src_col = col
        cat_col = col
        merged[src_col] = merged[src_col].where(~merged[cat_col].notna(), merged[cat_col])

    # For still-missing items, do best-effort classification
    needs_class = merged["category_l1"].isna() | merged["category_l2"].isna()
    if needs_class.any():
        tmp = merged.loc[needs_class, "product_name"].apply(best_effort_classify)
        merged.loc[needs_class, "category_l1"] = tmp.apply(lambda x: x[0])
        merged.loc[needs_class, "category_l2"] = tmp.apply(lambda x: x[1])
        # keep product_name as-is (x[2])

    # Unit price and total price
    # Prefer existing unit_price from source, else from catalog, else simple default 1.00
    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce")
    merged["unit_price"] = merged["unit_price"].fillna(merged["unit_price"])
    # If still NaN, try catalog price from merge
    merged["unit_price"] = merged["unit_price"].fillna(merged["unit_price"])
    if "unit_price" in catalog_df.columns:
        # The previous fill didn't change anything; ensure any NaNs become a default
        merged["unit_price"] = merged["unit_price"].fillna(1.00)

    # Total price if missing
    if "total_price" not in merged.columns:
        merged["total_price"] = merged["quantity"] * merged["unit_price"]
    else:
        merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce")
        merged["total_price"] = merged["total_price"].fillna(merged["quantity"] * merged["unit_price"])
    merged["total_price"] = merged["total_price"].round(2)

    # Final selection/order
    out_cols = [
        "transaction_id","timestamp","user_id","first_name","last_name","email","address",
        "category_l1","category_l2","product_name","quantity","unit_price","total_price"
    ]
    # Rename product_name -> category_l3 to match your earlier schema
    merged = merged.rename(columns={"product_name":"category_l3"})
    out_cols = [c if c != "product_name" else "category_l3" for c in out_cols]

    # Ensure all columns exist
    for c in out_cols:
        if c not in merged.columns:
            merged[c] = None

    out = merged[out_cols].copy()

    # Write outputs
    out.to_csv(args.out_csv, index=False)
    out.to_json(args.out_jsonl, orient="records", lines=True)

    print(f"Wrote {len(out)} rows to {args.out_csv} and {args.out_jsonl}")
    print(f"Catalog written to {args.catalog_out}")

if __name__ == "__main__":
    main()
