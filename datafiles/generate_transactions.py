#!/usr/bin/env python3
"""
Generate synthetic retail transactions with real-ish product names and category hierarchy.
Inputs:
  - /mnt/data/customers_with_id.csv (expects first/last/address/email/user_id columns as in your file)
Outputs:
  - /mnt/data/transactions.csv
  - /mnt/data/transactions.jsonl
  - /mnt/data/product_catalog.csv
"""
import pandas as pd
import numpy as np
import random, uuid
from datetime import datetime, timedelta
from pathlib import Path

def normalize_email(row):
    email = str(row.get("EM_Email", "")).strip()
    if email.lower() in ("", "empty", "nan"):
        first = str(row["Person_First_Name"]).strip().lower()
        last = str(row["Person_Last_Name"]).strip().lower()
        email = f"{first}.{last}.{str(row['user_id'])[:8]}@example.com".replace(" ", "")
    return email

def full_address(row):
    addr1 = str(row.get("Primary_Address","")).strip()
    city = str(row.get("City","")).strip()
    state = str(row.get("State","")).strip()
    zip4 = str(row.get("zip4","")).strip()
    parts = [p for p in [addr1, city, state] if p and p.lower() != "all others"]
    if zip4 and zip4.lower() != "all others":
        parts.append(zip4)
    return ", ".join(parts) if parts else "N/A"

def build_catalog():
    catalog = [('SOFT DRINKS', 'COKE', 'Coca-Cola Classic 12oz can', 1.09), ('SOFT DRINKS', 'COKE', 'Diet Coke 12oz can', 1.09), ('SOFT DRINKS', 'COKE', 'Coke Zero Sugar 12oz can', 1.09), ('SOFT DRINKS', 'DR PEPPER', 'Dr Pepper 12oz can', 1.09), ('SOFT DRINKS', 'DR PEPPER', 'Diet Dr Pepper 12oz can', 1.09), ('SOFT DRINKS', 'PEPSI', 'Pepsi 12oz can', 1.09), ('SOFT DRINKS', 'PEPSI', 'Diet Pepsi 12oz can', 1.09), ('SOFT DRINKS', 'PEPSICO', 'Mountain Dew 12oz can', 1.09), ('SOFT DRINKS', 'PEPSICO', 'Starry Lemon Lime 12oz can', 1.09), ('WATER', 'BOTTLED', 'Aquafina 16.9oz', 1.19), ('WATER', 'BOTTLED', 'Dasani 16.9oz', 1.19), ('SPORTS DRINKS', 'GATORADE', 'Gatorade Cool Blue 20oz', 1.79), ('SPORTS DRINKS', 'BODYARMOR', 'BodyArmor Fruit Punch 16oz', 1.99), ('SNACKS', 'CHIPS', "Lay's Classic 8oz", 3.99), ('SNACKS', 'CHIPS', 'Doritos Nacho Cheese 9.25oz', 4.29), ('SNACKS', 'CHIPS', 'Cheetos Crunchy 8.5oz', 4.19), ('SNACKS', 'CANDY', 'Snickers Bar 1.86oz', 1.49), ('SNACKS', 'CANDY', "M&M's Peanut 3.27oz", 1.89), ('SNACKS', 'CANDY', "Reese's Peanut Butter Cups 1.5oz", 1.49), ('GROCERY', 'CEREAL', 'Cheerios 18oz', 5.49), ('GROCERY', 'CEREAL', "Kellogg's Frosted Flakes 19oz", 5.79), ('GROCERY', 'PASTA', 'Barilla Spaghetti 1lb', 1.79), ('GROCERY', 'SAUCE', "Rao's Marinara 24oz", 7.99), ('PRODUCE', 'FRUIT', 'Bananas (lb)', 0.69), ('PRODUCE', 'FRUIT', 'Honeycrisp Apples (lb)', 2.49), ('PRODUCE', 'VEGGIES', 'Baby Carrots 1lb', 1.79), ('DAIRY', 'MILK', 'Horizon Organic Whole Milk Half Gallon', 5.49), ('DAIRY', 'YOGURT', 'Chobani Greek Yogurt 5.3oz', 1.39), ('DAIRY', 'CHEESE', 'Sargento Sliced Cheddar 8oz', 3.99), ('BAKERY', 'BREAD', 'Wonder Classic White Bread 20oz', 3.49), ('BAKERY', 'BAGELS', "Thomas' Plain Bagels 6ct", 4.29), ('HOUSEHOLD', 'CLEANING', 'Tide PODS 42ct', 13.99), ('HOUSEHOLD', 'DISH', 'Dawn Dish Soap 19.4oz', 3.29), ('HOUSEHOLD', 'PAPER', 'Bounty Paper Towels 6 Double Rolls', 9.99), ('PERSONAL CARE', 'ORAL CARE', 'Crest 3D White Toothpaste 4.1oz', 3.99), ('PERSONAL CARE', 'ORAL CARE', 'Colgate Total Toothpaste 4.8oz', 3.99), ('PERSONAL CARE', 'SHAMPOO', 'Head & Shoulders Classic Clean 13.5oz', 6.99), ('COFFEE & TEA', 'COFFEE', 'Starbucks Pike Place Roast 12oz', 9.99), ('COFFEE & TEA', 'COFFEE', "Peet's Major Dickason's Blend 12oz", 10.49)]
    return pd.DataFrame(catalog, columns=["category_l1","category_l2","product_name","unit_price"])

def generate_transactions(customers_csv="customers_with_id.csv", out_csv="transactions.csv", out_jsonl="transactions.jsonl", catalog_out="product_catalog.csv", n_rows=1000, start_days_ago=45):
    customers = pd.read_csv(customers_csv)
    customers_out = customers.rename(columns={
        "Person_First_Name":"first_name",
        "Person_Last_Name":"last_name",
        "EM_Email":"email",
    })
    customers_out["email"] = customers.apply(normalize_email, axis=1)
    customers_out["address"] = customers.apply(full_address, axis=1)

    catalog_df = build_catalog()
    catalog_df.to_csv(catalog_out, index=False)

    category_weights = {'SOFT DRINKS': 0.22, 'SPORTS DRINKS': 0.05, 'WATER': 0.06, 'SNACKS': 0.22, 'GROCERY': 0.16, 'PRODUCE': 0.08, 'DAIRY': 0.08, 'BAKERY': 0.05, 'HOUSEHOLD': 0.04, 'PERSONAL CARE': 0.02, 'COFFEE & TEA': 0.02}
    weights_series = catalog_df["category_l1"].map(category_weights).fillna(0.03)

    now = datetime.utcnow()
    rows = []
    for _ in range(n_rows):
        cust = customers_out.sample(1).iloc[0]
        prod = catalog_df.sample(1, weights=weights_series).iloc[0]
        qty = int(np.random.choice([1,1,1,2,2,3]))
        ts = now - timedelta(days=random.random()*start_days_ago, hours=random.randint(0,23), minutes=random.randint(0,59), seconds=random.randint(0,59))
        unit_price = float(prod["unit_price"])
        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(timespec="seconds")+"Z",
            "user_id": cust["user_id"],
            "first_name": cust["first_name"],
            "last_name": cust["last_name"],
            "email": cust["email"],
            "address": cust["address"],
            "category_l1": prod["category_l1"],
            "category_l2": prod["category_l2"],
            "category_l3": prod["product_name"],
            "quantity": qty,
            "unit_price": unit_price,
            "total_price": round(unit_price*qty, 2),
        })
    out = pd.DataFrame(rows)
    out.to_csv(out_csv, index=False)
    out.to_json(out_jsonl, orient="records", lines=True)
    print(f"Wrote {len(out)} rows to {out_csv} and {out_jsonl}")

if __name__ == "__main__":
    generate_transactions()
