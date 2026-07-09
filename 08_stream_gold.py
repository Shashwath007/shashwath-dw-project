# Databricks notebook source
# =============================================================
# CELL 1 — Setup and Configuration
# =============================================================

spark.sql("USE CATALOG `internship-proj1`")
spark.sql("USE SCHEMA default")

CLEAN_TABLE  = "streaming_orders_clean"
GOLD_TABLE   = "streaming_orders_gold"

# New checkpoint path for gold — must be different from silver's
CHECKPOINT_PATH = "/Volumes/internship-proj1/default/streaming_checkpoints/gold"

print("✅ Gold configuration loaded.")
print(f"   Source     : {CLEAN_TABLE}")
print(f"   Target     : {GOLD_TABLE}")
print(f"   Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================
# CELL 2 — Schema for streaming_orders_clean
# =============================================================
# We need to tell the stream reader what columns to expect.
# This matches what 07_stream_silver writes — same 21 columns
# PLUS the 2 new ones silver added (processed_at, data_source)
# =============================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    DateType, TimestampType
)

CLEAN_SCHEMA = StructType([
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
    StructField("processed_at",  TimestampType(),True),  # added by silver
    StructField("data_source",   StringType(),  True),   # added by silver
])

print(f"✅ Clean schema defined with {len(CLEAN_SCHEMA.fields)} columns.")

# COMMAND ----------

# =============================================================
# CELL 3 — Pre-create the Gold Table
# =============================================================
# We create the gold table upfront with the exact structure
# we want. This way the stream just overwrites it each run
# with fresh aggregated numbers.
#
# WHY overwrite instead of append?
# Aggregations change existing numbers — if Furniture/West
# had $12,000 total sales and 3 new orders come in, the total
# becomes $14,500. We can't append that — we must replace it.
# =============================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        category        STRING,
        region          STRING,
        total_sales     DOUBLE,
        total_orders    BIGINT,
        total_profit    DOUBLE,
        avg_discount    DOUBLE,
        last_updated    TIMESTAMP
    )
    USING DELTA
""")

print(f"✅ Gold table ready: {GOLD_TABLE}")
print("\n📋 Current gold table (empty on first run):")
spark.read.table(GOLD_TABLE).show()

# COMMAND ----------

# =============================================================
# CELL 4 — Gold Aggregation Loop
# =============================================================
# Every 20 seconds this:
# 1. Reads ALL rows from streaming_orders_clean (batch read)
# 2. Groups by category + region
# 3. Computes running totals
# 4. Overwrites streaming_orders_gold with fresh numbers
#
# WHY batch read here instead of readStream?
# Because we want COMPLETE aggregates (all data, every run).
# readStream with aggregations in append mode would only give
# us incremental counts — harder to use in Power BI.
# This "batch-on-a-loop" pattern is simpler and works perfectly
# for a dashboard refresh every 20 seconds.
# =============================================================

import time
from pyspark.sql import functions as F

LOOP_INTERVAL = 20
run_count = 0

print("🥇 Gold aggregation loop started!")
print(f"   Recalculates every {LOOP_INTERVAL} seconds.")
print("🛑 To stop: click the STOP (■) button.\n")

while True:
    try:
        run_count += 1
        print(f"⚡ Run #{run_count:04d} — {time.strftime('%H:%M:%S')} — aggregating...")

        # Step 1: Read ALL clean data (batch, not stream)
        clean_df = spark.read.table(CLEAN_TABLE)
        total_input_rows = clean_df.count()

        # Step 2: Aggregate by category + region
        gold_df = (
            clean_df
            .groupBy("category", "region")
            .agg(
                F.round(F.sum("sales"),    2).alias("total_sales"),
                F.count("order_id")         .alias("total_orders"),
                F.round(F.sum("profit"),   2).alias("total_profit"),
                F.round(F.avg("discount"), 4).alias("avg_discount"),
                F.current_timestamp()       .alias("last_updated")
            )
            .orderBy("category", "region")
        )

        # Step 3: Overwrite gold table with fresh aggregates
        (gold_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(GOLD_TABLE))

        # Step 4: Log summary
        gold_row_count = gold_df.count()
        print(f"   ✅ Done — {total_input_rows} clean rows → "
              f"{gold_row_count} gold aggregates")

        # Step 5: Show a snapshot every 3 runs
        if run_count % 3 == 0:
            print(f"\n   📊 Gold table snapshot (run #{run_count}):")
            spark.read.table(GOLD_TABLE).show(truncate=False)

        time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n🛑 Gold loop stopped after {run_count} runs.")
        break
    except Exception as e:
        print(f"   ❌ Error on run #{run_count}: {e}")
        print("   Retrying in 20 seconds...")
        time.sleep(20)