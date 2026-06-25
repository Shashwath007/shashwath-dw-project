# Databricks notebook source
# ============================================
# NOTEBOOK 3 - GOLD LAYER
# Build Star Schema: 1 Fact + 4 Dimensions
# ============================================

from pyspark.sql.functions import (
    col, year, month, quarter, dayofmonth,
    monotonically_increasing_id, date_format
)

# Read Silver table
df = spark.table("`internship-proj1`.default.silver_superstore")
print("Silver row count:", df.count())

# -----------------------------------------------
# DIM 1: DimDate
# -----------------------------------------------
df_dates = df.select("order_date").distinct()

dim_date = df_dates \
  .withColumn("date_key", date_format(col("order_date"), "yyyyMMdd").cast("integer")) \
  .withColumn("year", year(col("order_date"))) \
  .withColumn("month", month(col("order_date"))) \
  .withColumn("quarter", quarter(col("order_date"))) \
  .withColumn("day", dayofmonth(col("order_date"))) \
  .withColumn("month_name", date_format(col("order_date"), "MMMM")) \
  .orderBy("order_date")

dim_date.write.format("delta").mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.gold_dim_date")
print("✅ DimDate done:", dim_date.count(), "rows")

# -----------------------------------------------
# DIM 2: DimCustomer
# -----------------------------------------------
dim_customer = df.select(
  "customer_id", "customer_name", "segment"
).distinct() \
  .withColumn("customer_key", monotonically_increasing_id())

dim_customer.write.format("delta").mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.gold_dim_customer")
print("✅ DimCustomer done:", dim_customer.count(), "rows")

# -----------------------------------------------
# DIM 3: DimProduct
# -----------------------------------------------
dim_product = df.select(
  "product_id", "product_name", "category", "sub_category"
).distinct() \
  .withColumn("product_key", monotonically_increasing_id())

dim_product.write.format("delta").mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.gold_dim_product")
print("✅ DimProduct done:", dim_product.count(), "rows")

# -----------------------------------------------
# DIM 4: DimRegion
# -----------------------------------------------
dim_region = df.select(
  "region", "state", "city"
).distinct() \
  .withColumn("region_key", monotonically_increasing_id())

dim_region.write.format("delta").mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.gold_dim_region")
print("✅ DimRegion done:", dim_region.count(), "rows")

# -----------------------------------------------
# FACT TABLE: FactOrders
# -----------------------------------------------

# Join all dimension keys onto the silver data
fact = df \
  .join(
    dim_date.select("order_date", "date_key"),
    on="order_date", how="left"
  ) \
  .join(
    dim_customer.select("customer_id", "customer_key"),
    on="customer_id", how="left"
  ) \
  .join(
    dim_product.select("product_id", "product_key"),
    on="product_id", how="left"
  ) \
  .join(
    dim_region.select("region", "state", "city", "region_key"),
    on=["region", "state", "city"], how="left"
  ) \
  .select(
    "order_id",
    "ship_mode",
    "date_key",
    "customer_key",
    "product_key",
    "region_key",
    "sales",
    "quantity",
    "discount",
    "profit"
  )

fact.write.format("delta").mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("`internship-proj1`.default.gold_fact_orders")

print("\n🏆 Gold layer complete! Star schema ready.")
print("Tables created:")
print("  - gold_dim_date")
print("  - gold_dim_customer")
print("  - gold_dim_product")
print("  - gold_dim_region")
print("  - gold_fact_orders")



# COMMAND ----------

# ============================================
# SCD TYPE 1 - DimCustomer
# Overwrite changed records with latest values
# ============================================

from pyspark.sql.functions import col

# Step 1: Read existing DimCustomer (current state)
dim_customer_existing = spark.table("`internship-proj1`.default.gold_dim_customer")

# Step 2: Get latest customer data from Silver
dim_customer_new = spark.table("`internship-proj1`.default.silver_superstore") \
    .select("customer_id", "customer_name", "segment") \
    .distinct()

# Step 3: Join existing with new to find changes
from pyspark.sql.functions import when

merged = dim_customer_existing.alias("existing") \
    .join(dim_customer_new.alias("new"), on="customer_id", how="left") \
    .select(
        col("existing.customer_key"),
        col("existing.customer_id"),
        # SCD Type 1: overwrite name and segment with latest values
        col("new.customer_name"),
        col("new.segment")
    )

# Step 4: Save back — overwriting old records
merged.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`internship-proj1`.default.gold_dim_customer")

print("✅ SCD Type 1 applied on DimCustomer!")
print("Updated row count:", merged.count())

# COMMAND ----------

