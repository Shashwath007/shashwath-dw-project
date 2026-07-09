# Databricks notebook source
# =============================================================
# ONE-TIME SETUP — Create a Volume for checkpoint storage
# Run this ONCE, then never again
# =============================================================

spark.sql("USE CATALOG `internship-proj1`")
spark.sql("USE SCHEMA default")

# Create a Volume — this is a managed folder inside Unity Catalog
# Volumes replace DBFS for storing files in modern Databricks
spark.sql("CREATE VOLUME IF NOT EXISTS streaming_checkpoints")

print("✅ Volume created: internship-proj1.default.streaming_checkpoints")

# Verify it exists
spark.sql("SHOW VOLUMES").show()

# COMMAND ----------

# =============================================================
# CELL 1 — Setup and Configuration (FIXED checkpoint path)
# =============================================================

spark.sql("USE CATALOG `internship-proj1`")
spark.sql("USE SCHEMA default")

RAW_TABLE   = "streaming_orders_raw"
CLEAN_TABLE = "streaming_orders_clean"

# FIX: Use Unity Catalog Volume path instead of DBFS
# Format: /Volumes/<catalog>/<schema>/<volume>/<subfolder>
CHECKPOINT_PATH = "/Volumes/internship-proj1/default/streaming_checkpoints/silver"

print("✅ Configuration loaded.")
print(f"   Source     : {RAW_TABLE}")
print(f"   Target     : {CLEAN_TABLE}")
print(f"   Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================
# CELL 2 — Define the Expected Schema
# =============================================================
# Structured Streaming REQUIRES you to tell it the schema
# (column names + types) upfront. It cannot infer schema
# from a streaming source the way batch can.
#
# This must exactly match what the generator writes.
# =============================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

RAW_SCHEMA = StructType([
    StructField("row_id",        IntegerType(), True),
    StructField("order_id",      StringType(),  True),
    StructField("order_date",    DateType(),    True),
    StructField("ship_date",     DateType(),    True),
    StructField("ship_mode",     StringType(),  True),
    StructField("customer_id",   StringType(),  True),
    StructField("customer_name", StringType(),  True),
    StructField("segment",       StringType(),  True),
    StructField("country",       StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("postal_code",   StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("product_id",    StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("sub_category",  StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("sales",         DoubleType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("discount",      DoubleType(),  True),
    StructField("profit",        DoubleType(),  True),
])

print(f"✅ Schema defined with {len(RAW_SCHEMA.fields)} columns.")
print("\n📋 Column list:")
for field in RAW_SCHEMA.fields:
    print(f"   {field.name:20s} → {field.dataType}")

# COMMAND ----------

# =============================================================
# CELL 3 — Silver Cleaning Transformations
# =============================================================
# This function takes a raw streaming DataFrame and returns
# a cleaned one. Same logic as your 02_silver.py but adapted
# for streaming (no aggregations, just row-level transforms).
# =============================================================

from pyspark.sql import functions as F

def apply_silver_cleaning(df):
    """
    Applies Silver-layer cleaning to a streaming DataFrame.
    
    Rules applied:
    1. Drop rows where critical fields are null
    2. Ensure sales, quantity, profit are valid numbers
    3. Trim whitespace from all string columns
    4. Add a processed_at timestamp so we know when each
       row was cleaned (useful for debugging + Power BI)
    5. Add a data_source tag to distinguish simulated orders
    """

    # --- Step 1: Drop rows missing critical fields ---
    # If order_id or sales is null, the row is useless
    df_clean = df.filter(
        F.col("order_id").isNotNull() &
        F.col("sales").isNotNull() &
        F.col("order_date").isNotNull() &
        F.col("customer_id").isNotNull()
    )

    # --- Step 2: Validate numeric ranges ---
    # sales and quantity must be positive
    # discount must be between 0 and 1
    df_clean = df_clean.filter(
        (F.col("sales") > 0) &
        (F.col("quantity") > 0) &
        (F.col("discount") >= 0) &
        (F.col("discount") <= 1)
    )

    # --- Step 3: Trim whitespace from string columns ---
    string_cols = [
        "order_id", "ship_mode", "customer_id", "customer_name",
        "segment", "country", "city", "state", "postal_code",
        "region", "product_id", "category", "sub_category", "product_name"
    ]
    for col_name in string_cols:
        df_clean = df_clean.withColumn(col_name, F.trim(F.col(col_name)))

    # --- Step 4: Add processed_at timestamp ---
    # This records exactly when this row passed through Silver
    df_clean = df_clean.withColumn(
        "processed_at",
        F.current_timestamp()
    )

    # --- Step 5: Tag the data source ---
    df_clean = df_clean.withColumn(
        "data_source",
        F.lit("streaming_simulated")
    )

    return df_clean

print("✅ Silver cleaning function defined.")
print("   Rules: null checks, range validation, whitespace trim,")
print("          processed_at timestamp, data_source tag.")

# COMMAND ----------

# =============================================================
# CELL 4 — Silver Stream (FIXED for Serverless/Community Edition)
# =============================================================
# Serverless doesn't support infinite processingTime streams.
# Instead we use availableNow=True which:
#   - Processes ALL rows not yet seen
#   - Finishes cleanly
#   - We re-trigger it every 15 seconds in a loop
#
# This gives the same "near real-time" effect for our dashboard.
# =============================================================

import time
from pyspark.sql import functions as F

LOOP_INTERVAL = 15   # re-trigger stream every 15 seconds
run_count = 0

print("🔄 Silver stream loop started (Serverless-compatible mode)")
print(f"   Re-triggers every {LOOP_INTERVAL} seconds.")
print("🛑 To stop: click the STOP (■) button on this cell.\n")

while True:
    try:
        run_count += 1
        print(f"⚡ Run #{run_count:04d} — {time.strftime('%H:%M:%S')} — processing new rows...")

        # Step 1: Read ALL new rows not yet processed
        raw_stream_df = (
            spark.readStream
                 .format("delta")
                 .schema(RAW_SCHEMA)
                 .table(RAW_TABLE)
        )

        # Step 2: Apply Silver cleaning
        clean_stream_df = apply_silver_cleaning(raw_stream_df)

        # Step 3: Write with availableNow=True
        # .awaitTermination() blocks until this batch finishes,
        # then we sleep and loop again
        silver_query = (
            clean_stream_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_PATH)
                .trigger(availableNow=True)      # ← THE FIX
                .toTable(CLEAN_TABLE)
        )

        # Wait for this batch to fully finish before looping
        silver_query.awaitTermination()
        print(f"   ✅ Run #{run_count:04d} complete.")

        # Sleep before next run
        time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n🛑 Silver stream stopped after {run_count} runs.")
        break
    except Exception as e:
        print(f"   ❌ Error on run #{run_count}: {e}")
        print("   Retrying in 20 seconds...")
        time.sleep(20)

# COMMAND ----------

