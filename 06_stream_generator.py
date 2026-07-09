# Databricks notebook source
# =============================================================
# CELL 1 — Imports and Configuration (FIXED)
# =============================================================

import random
import time
import uuid
from datetime import datetime, timedelta

from pyspark.sql import Row

# ------------------------------------------------------------------
# FIX: Set catalog + schema context on the Spark session directly.
# This means we NEVER have to type the full 3-part name again.
# Spark will treat all table operations as being inside this catalog+schema.
# ------------------------------------------------------------------
spark.sql("USE CATALOG `internship-proj1`")
spark.sql("USE SCHEMA default")

# Now we can use just the short table name — no hyphens in the path
RAW_TABLE = "streaming_orders_raw"

print(f"✅ Catalog set to : internship-proj1")
print(f"✅ Schema set to  : default")
print(f"📦 Orders will be written to: {RAW_TABLE}")

# COMMAND ----------

# =============================================================
# CELL 2 — Fake Order Generator Function
# =============================================================
# What this does: defines a function that returns ONE fake
# Superstore order as a dictionary. Values are randomized
# but realistic — same columns as your real dataset.
# =============================================================

# ---- Lookup lists (sampled from real Superstore data) --------

SHIP_MODES    = ["First Class", "Second Class", "Standard Class", "Same Day"]

CUSTOMERS     = [
    ("CG-12520", "Claire Gute",      "Consumer"),
    ("DV-13045", "Darrin Van Huff",  "Corporate"),
    ("SO-20335", "Sean O'Donnell",   "Consumer"),
    ("BH-11710", "Brosina Hoffman",  "Consumer"),
    ("AA-10480", "Andrew Allen",     "Corporate"),
    ("IM-15070", "Irene Maddox",     "Consumer"),
    ("HP-14815", "Harold Pawlan",    "Home Office"),
    ("PK-19075", "Pete Kriz",        "Consumer"),
    ("AG-10270", "Alejandro Grove",  "Corporate"),
    ("ZD-21925", "Zuschuss Donatelli","Consumer"),
]

REGIONS_CITIES = [
    ("South",   "Henderson",    "Kentucky",       "42420"),
    ("South",   "Fort Lauderdale","Florida",      "33311"),
    ("West",    "Los Angeles",  "California",     "90036"),
    ("East",    "Concord",      "North Carolina", "28027"),
    ("West",    "Seattle",      "Washington",     "98103"),
    ("Central", "Chicago",      "Illinois",       "60623"),
    ("East",    "New York City","New York",       "10024"),
    ("South",   "San Antonio",  "Texas",          "78207"),
    ("West",    "Phoenix",      "Arizona",        "85023"),
    ("Central", "Minneapolis",  "Minnesota",      "55407"),
]

PRODUCTS = [
    ("FUR-BO-10001798", "Furniture",        "Bookcases",   "Bush Somerset Collection Bookcase"),
    ("FUR-CH-10000454", "Furniture",        "Chairs",      "Hon Deluxe Fabric Upholstered Stacking Chairs"),
    ("OFF-LA-10000240", "Office Supplies",  "Labels",      "Self-Adhesive Address Labels"),
    ("FUR-TA-10000577", "Furniture",        "Tables",      "Bretford CR4500 Series Slim Rectangular Table"),
    ("OFF-ST-10000760", "Office Supplies",  "Storage",     "Eldon Fold 'N Roll Cart System"),
    ("TEC-PH-10002033", "Technology",       "Phones",      "Mitel 5320 IP Phone VoIP phone"),
    ("OFF-BI-10003910", "Office Supplies",  "Binders",     "DXL Angle-View Binders with Locking Rings"),
    ("TEC-AC-10004070", "Technology",       "Accessories", "Enermax Aurora Lite Case"),
    ("TEC-CO-10004722", "Technology",       "Copiers",     "Canon PC1080F Personal Copier"),
    ("OFF-PA-10001569", "Office Supplies",  "Paper",       "Easy-staple paper"),
]

def generate_fake_order():
    """
    Returns a dict representing one fake Superstore order.
    Matches the exact column structure of your existing dataset.
    """
    # Pick random values from lookup lists
    cust_id, cust_name, segment    = random.choice(CUSTOMERS)
    region, city, state, postal    = random.choice(REGIONS_CITIES)
    prod_id, category, sub_cat, prod_name = random.choice(PRODUCTS)

    # Generate order date = somewhere in last 30 days
    days_ago   = random.randint(0, 30)
    order_date = datetime.today() - timedelta(days=days_ago)
    ship_date  = order_date + timedelta(days=random.randint(1, 7))

    # Generate financials
    sales    = round(random.uniform(10.0, 1500.0), 2)
    quantity = random.randint(1, 10)
    discount = random.choice([0.0, 0.1, 0.2, 0.3, 0.4])
    profit   = round(sales * random.uniform(-0.1, 0.4), 2)

    # Unique order ID
    order_id = f"SIM-{uuid.uuid4().hex[:8].upper()}"
    row_id   = random.randint(10000, 99999)

    return {
        "row_id":       row_id,
        "order_id":     order_id,
        "order_date":   order_date.date(),
        "ship_date":    ship_date.date(),
        "ship_mode":    random.choice(SHIP_MODES),
        "customer_id":  cust_id,
        "customer_name":cust_name,
        "segment":      segment,
        "country":      "United States",
        "city":         city,
        "state":        state,
        "postal_code":  postal,
        "region":       region,
        "product_id":   prod_id,
        "category":     category,
        "sub_category": sub_cat,
        "product_name": prod_name,
        "sales":        sales,
        "quantity":     quantity,
        "discount":     discount,
        "profit":       profit,
    }

# Quick test — print one sample order
sample = generate_fake_order()
for k, v in sample.items():
    print(f"  {k:20s}: {v}")

# COMMAND ----------

# =============================================================
# CELL 3 — Infinite Generator Loop (FIXED)
# =============================================================

WRITE_INTERVAL_SECONDS = 5
orders_written = 0

print("🚀 Generator started! Writing one order every "
      f"{WRITE_INTERVAL_SECONDS} seconds.")
print("🛑 To stop: click the STOP button (■) on this cell.\n")

while True:
    try:
        # 1. Generate one fake order
        order_dict = generate_fake_order()

        # 2. Wrap it in a Spark DataFrame (one row)
        order_row = Row(**order_dict)
        df = spark.createDataFrame([order_row])

        # 3. FIX: Use short table name only — catalog+schema already set in Cell 1
        (df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(RAW_TABLE))          # ← just "streaming_orders_raw"

        # 4. Log progress
        orders_written += 1
        print(f"  ✅ Order #{orders_written:04d} written | "
              f"ID: {order_dict['order_id']} | "
              f"Sales: ${order_dict['sales']:.2f} | "
              f"{datetime.now().strftime('%H:%M:%S')}")

        # 5. Wait before next order
        time.sleep(WRITE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n🛑 Generator stopped after {orders_written} orders.")
        break
    except Exception as e:
        print(f"❌ Error on order #{orders_written + 1}: {e}")
        print("   Retrying in 10 seconds...")
        time.sleep(10)

# COMMAND ----------

