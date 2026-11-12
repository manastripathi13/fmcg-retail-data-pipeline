"""Simulate the FMCG retailer's daily file drop into the landing bucket.

Every business day the retailer's systems export four gzipped CSV files:

  gs://<landing>/transactions/YYYY/MM/DD/transactions_YYYYMMDD.csv.gz   POS line items for that day
  gs://<landing>/customers/YYYY/MM/DD/customers_YYYYMMDD.csv.gz         loyalty members (full on day 1, then new + changed)
  gs://<landing>/stores/YYYY/MM/DD/stores_YYYYMMDD.csv.gz               full store master snapshot
  gs://<landing>/promotions/YYYY/MM/DD/promotions_YYYYMMDD.csv.gz       promotions live or announced that day

The data is synthetic but shaped like real Indian FMCG retail: festive demand building to Diwali
(8 Nov 2026), weekend peaks, promotions that lift units, returns (negative quantity), walk-in
customers without a loyalty id, and ~0.5% of lines re-sent twice by the POS system.

Usage:
    python generator/generate_fmcg_drop.py --date 2026-10-26          # one day (what Airflow runs)
    python generator/generate_fmcg_drop.py --all                      # every day of the simulation
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import math
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import LANDING_BUCKET, gcs_client  # noqa: E402

SIM_START, SIM_END = date(2026, 10, 26), date(2026, 11, 8)   # ISO weeks 44-45 of 2026, ending on Diwali
DIWALI = date(2026, 11, 8)
SEED = 2026

REGIONS = {
    "North": [("Delhi", "Delhi"), ("Gurugram", "Haryana"), ("Noida", "Uttar Pradesh"), ("Lucknow", "Uttar Pradesh"), ("Jaipur", "Rajasthan")],
    "West": [("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Ahmedabad", "Gujarat"), ("Surat", "Gujarat")],
    "South": [("Bengaluru", "Karnataka"), ("Chennai", "Tamil Nadu"), ("Hyderabad", "Telangana"), ("Kochi", "Kerala")],
    "East": [("Kolkata", "West Bengal"), ("Bhubaneswar", "Odisha"), ("Patna", "Bihar")],
}
FORMATS = {"Hypermarket": (28000, 60000, 3.0), "Supermarket": (8000, 20000, 1.6), "Convenience": (1500, 4000, 0.7), "Kirana": (400, 1200, 0.45)}

# category: [(sub_category, [(brand, product, pack, price_inr)])]
CATALOG = {
    "Personal Care": [
        ("Bath Soap", [("Lumina", "Sandal Glow Soap", "4x100g", 180), ("Neemora", "Neem Fresh Soap", "3x125g", 145), ("Aurelle", "Milk Cream Soap", "4x75g", 120)]),
        ("Shampoo", [("Aurelle", "Silk Repair Shampoo", "340ml", 325), ("Kesh", "Anti-Dandruff Shampoo", "180ml", 199), ("Neemora", "Herbal Shampoo", "650ml", 449)]),
        ("Toothpaste", [("Dantika", "Clove Guard Toothpaste", "200g", 115), ("SmileOn", "Whitening Gel", "150g", 135), ("Dantika", "Sensitive Care", "100g", 160)]),
        ("Hair Oil", [("Kesh", "Coconut Hair Oil", "500ml", 210), ("Kesh", "Amla Hair Oil", "300ml", 165)]),
    ],
    "Home Care": [
        ("Detergent", [("Brightex", "Matic Front Load Powder", "2kg", 499), ("Brightex", "Detergent Bar", "4x250g", 105), ("Washwell", "Liquid Detergent", "1L", 235)]),
        ("Dishwash", [("Glimmer", "Lemon Dishwash Bar", "3x200g", 60), ("Glimmer", "Dishwash Gel", "750ml", 189)]),
        ("Surface Care", [("Kleanso", "Floor Cleaner", "1L", 199), ("Kleanso", "Toilet Cleaner", "500ml", 99)]),
        ("Air Care", [("Fresca", "Room Freshener", "240ml", 225)]),
    ],
    "Foods": [
        ("Biscuits", [("Crunchy Co", "Glucose Biscuits", "800g", 90), ("Crunchy Co", "Cream Cookies", "300g", 60), ("Tasteland", "Digestive Biscuits", "1kg", 210)]),
        ("Instant Noodles", [("Zapp", "Masala Noodles", "12x70g", 168), ("Zapp", "Atta Noodles", "4x70g", 70)]),
        ("Snacks", [("Tasteland", "Aloo Bhujia", "1kg", 260), ("Tasteland", "Salted Chips", "130g", 50), ("Munchwell", "Mixed Namkeen", "400g", 115)]),
        ("Staples", [("Annapurna Gold", "Basmati Rice", "5kg", 650), ("Annapurna Gold", "Chakki Atta", "10kg", 520), ("Pureleaf", "Refined Sunflower Oil", "1L", 175)]),
        ("Sweets & Festive", [("Mithas", "Kaju Katli Box", "500g", 499), ("Mithas", "Assorted Dry Fruits", "1kg", 899), ("Mithas", "Soan Papdi", "500g", 160)]),
    ],
    "Beverages": [
        ("Tea", [("Chaiwala", "Premium Tea", "1kg", 560), ("Chaiwala", "Green Tea Bags", "100 pcs", 340)]),
        ("Coffee", [("Brewnest", "Instant Coffee", "200g", 480)]),
        ("Soft Drinks", [("Fizzup", "Cola", "2.25L", 99), ("Fizzup", "Lemon Soda", "6x300ml", 180)]),
        ("Juices", [("Orchard", "Mixed Fruit Juice", "1L", 125)]),
    ],
    "Dairy": [
        ("Milk", [("Gaukul", "Toned Milk", "1L", 66), ("Gaukul", "Full Cream Milk", "500ml", 36)]),
        ("Butter & Ghee", [("Gaukul", "Salted Butter", "500g", 285), ("Gaukul", "Cow Ghee", "1L", 650)]),
        ("Curd & Paneer", [("Gaukul", "Fresh Paneer", "200g", 95), ("Gaukul", "Thick Curd", "400g", 55)]),
    ],
}
FESTIVE_SUBCATS = {"Sweets & Festive", "Staples", "Air Care", "Surface Care", "Butter & Ghee"}

FIRST = ["Aarav", "Vivaan", "Aditya", "Ishaan", "Rohan", "Kabir", "Ananya", "Diya", "Saanvi", "Aditi", "Meera", "Priya",
         "Rahul", "Neha", "Pooja", "Arjun", "Karan", "Sneha", "Vikram", "Riya", "Farhan", "Zoya", "Harpreet", "Lakshmi"]
LAST = ["Sharma", "Verma", "Gupta", "Iyer", "Nair", "Reddy", "Patel", "Shah", "Das", "Banerjee", "Khan", "Singh", "Joshi", "Menon", "Rao"]


def products() -> list[dict]:
    out, n = [], 0
    for cat, subs in CATALOG.items():
        for sub, items in subs:
            for brand, name, pack, price in items:
                n += 1
                out.append({"sku": f"SKU{n:04d}", "product_name": name, "brand": brand, "category": cat,
                            "sub_category": sub, "pack_size": pack, "mrp": float(price)})
    return out


def build_stores(rng: random.Random) -> list[dict]:
    stores, n = [], 0
    for region, cities in REGIONS.items():
        for city, state in cities:
            for _ in range(rng.choice([2, 2, 3, 3, 4])):
                n += 1
                fmt = rng.choices(list(FORMATS), weights=[2, 4, 3, 3])[0]
                lo, hi, _ = FORMATS[fmt]
                stores.append({"store_id": f"ST{n:03d}", "store_name": f"{city} {fmt} {n}", "store_format": fmt,
                               "city": city, "state": state, "region": region,
                               "opened_date": (date(2012, 1, 1) + timedelta(days=rng.randint(0, 4800))).isoformat(),
                               "area_sqft": rng.randint(lo, hi)})
    return stores


def build_customers(rng: random.Random, stores: list[dict], n: int = 3000) -> list[dict]:
    out = []
    for i in range(1, n + 1):
        home = rng.choice(stores)
        out.append({"customer_id": f"CU{i:06d}", "customer_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                    "gender": rng.choice(["F", "M"]), "age_band": rng.choices(["18-24", "25-34", "35-44", "45-54", "55+"], weights=[2, 4, 4, 3, 2])[0],
                    "city": home["city"], "state": home["state"], "home_store_id": home["store_id"],
                    "loyalty_tier": rng.choices(["Bronze", "Silver", "Gold", "Platinum"], weights=[50, 30, 15, 5])[0],
                    "signup_date": (date(2019, 1, 1) + timedelta(days=rng.randint(0, 2490))).isoformat()})
    return out


def build_promotions() -> list[dict]:
    p = [
        ("PR001", "Diwali Sweets Bonanza", "PERCENT_OFF", 20, "Foods", "Mithas", "2026-10-30", "2026-11-08"),
        ("PR002", "Festive Clean Home", "PERCENT_OFF", 15, "Home Care", "Kleanso", "2026-10-26", "2026-11-08"),
        ("PR003", "Brightex Wash Week", "FLAT_OFF", 10, "Home Care", "Brightex", "2026-10-26", "2026-11-01"),
        ("PR004", "Staples Stock-Up", "PERCENT_OFF", 10, "Foods", "Annapurna Gold", "2026-11-02", "2026-11-08"),
        ("PR005", "Chai Time", "BUY_X_GET_Y", 25, "Beverages", "Chaiwala", "2026-10-29", "2026-11-04"),
        ("PR006", "Ghee for Diwali", "PERCENT_OFF", 12, "Dairy", "Gaukul", "2026-11-03", "2026-11-08"),
        ("PR007", "Glow Up Personal Care", "PERCENT_OFF", 18, "Personal Care", "Aurelle", "2026-10-26", "2026-11-02"),
        ("PR008", "Snack Attack", "BUY_X_GET_Y", 33, "Foods", "Tasteland", "2026-11-05", "2026-11-08"),
        ("PR009", "Fizzup Party Pack", "FLAT_OFF", 8, "Beverages", "Fizzup", "2026-11-06", "2026-11-08"),
    ]
    return [{"promotion_id": a, "promo_name": b, "promo_type": c, "discount_pct": d, "category": e, "brand": f,
             "start_date": g, "end_date": h} for a, b, c, d, e, f, g, h in p]


def demand_multiplier(d: date) -> float:
    days_to_diwali = (DIWALI - d).days
    festive = 1 + 0.9 * math.exp(-max(days_to_diwali, 0) / 4) if days_to_diwali >= 0 else 1.0
    weekend = 1.35 if d.weekday() >= 5 else 1.0
    return festive * weekend


def transactions_for(d: date, rng: random.Random, stores, customers, prods, promos) -> list[dict]:
    live = [p for p in promos if p["start_date"] <= d.isoformat() <= p["end_date"]]
    by_store = {}
    for c in customers:
        by_store.setdefault(c["home_store_id"], []).append(c)
    rows, mult = [], demand_multiplier(d)
    for st in stores:
        baskets = int(rng.gauss(14, 3) * FORMATS[st["store_format"]][2] * mult)
        for b in range(max(baskets, 1)):
            hour = rng.choices(range(8, 23), weights=[1, 2, 3, 4, 4, 4, 3, 3, 4, 6, 7, 7, 6, 4, 2])[0]
            ts = datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59))
            member = by_store.get(st["store_id"]) if rng.random() < 0.7 else None
            cust = rng.choice(member)["customer_id"] if member else ""
            txn_id = f"T{d:%Y%m%d}{st['store_id'][2:]}{b:04d}"
            channel = "online" if rng.random() < 0.12 else "in_store"
            pay = rng.choices(["UPI", "Card", "Cash", "Wallet"], weights=[50, 22, 20, 8])[0]
            for line in range(1, rng.randint(2, 7)):
                pr = rng.choice(prods)
                if pr["sub_category"] in FESTIVE_SUBCATS and rng.random() < 0.35 * (mult - 1):
                    pr = rng.choice([x for x in prods if x["sub_category"] in FESTIVE_SUBCATS])
                promo = next((p for p in live if p["category"] == pr["category"] and p["brand"] == pr["brand"]), None)
                qty = rng.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0] + (1 if promo and rng.random() < 0.5 else 0)
                if rng.random() < 0.01:
                    qty = -qty  # return
                price = pr["mrp"]
                disc = round(abs(qty) * price * promo["discount_pct"] / 100, 2) if promo else 0.0
                rows.append({"transaction_id": txn_id, "line_no": line, "transaction_ts": ts.isoformat(sep=" "),
                             "store_id": st["store_id"], "customer_id": cust, "sku": pr["sku"],
                             "product_name": pr["product_name"], "brand": pr["brand"], "category": pr["category"],
                             "sub_category": pr["sub_category"], "pack_size": pr["pack_size"], "quantity": qty,
                             "unit_price": price, "discount_amount": disc if qty > 0 else -disc,
                             "promotion_id": promo["promotion_id"] if promo else "", "payment_mode": pay, "channel": channel})
    # the POS sometimes re-sends a line (exact duplicate) - the mart must de-duplicate
    rows += [dict(r) for r in rng.sample(rows, max(1, len(rows) // 200))]
    rng.shuffle(rows)
    return rows


def to_csv_gz(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode())


def drop_for_day(d: date) -> dict[str, int]:
    base = random.Random(SEED)            # master data is identical on every run
    stores = build_stores(base)
    customers = build_customers(base, stores)
    prods = products()
    promos = build_promotions()
    rng = random.Random(SEED * 1000 + d.toordinal())  # each day reproducible on its own
    day_no = (d - SIM_START).days

    # customers: full extract on day 1, then the members who changed tier or signed up "today"
    if day_no == 0:
        cust_rows = customers
    else:
        cust_rows = [dict(c, loyalty_tier=rng.choice(["Silver", "Gold", "Platinum"])) for c in rng.sample(customers, 25)]
        cust_rows += [c for c in customers if c["signup_date"] == d.isoformat()]
    stamp = f"{d.isoformat()} 23:30:00"
    files = {
        "transactions": transactions_for(d, rng, stores, customers, prods, promos),
        "customers": [dict(c, updated_at=stamp) for c in cust_rows],
        "stores": [dict(s, updated_at=stamp) for s in stores],
        "promotions": [dict(p, updated_at=stamp) for p in promos if p["start_date"] <= (d + timedelta(days=3)).isoformat() and p["end_date"] >= d.isoformat()],
    }
    bucket = gcs_client().bucket(LANDING_BUCKET)
    counts = {}
    for table, rows in files.items():
        path = f"{table}/{d:%Y/%m/%d}/{table}_{d:%Y%m%d}.csv.gz"
        bucket.blob(path).upload_from_string(to_csv_gz(rows), content_type="application/gzip")
        counts[table] = len(rows)
    return counts


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=date.fromisoformat)
    g.add_argument("--all", action="store_true")
    a = ap.parse_args()
    days = [SIM_START + timedelta(days=i) for i in range((SIM_END - SIM_START).days + 1)] if a.all else [a.date]
    for d in days:
        counts = drop_for_day(d)
        print(f"landed {d} -> gs://{LANDING_BUCKET}/*/{d:%Y/%m/%d}/ " + " ".join(f"{k}={v}" for k, v in counts.items()))


if __name__ == "__main__":
    main()
