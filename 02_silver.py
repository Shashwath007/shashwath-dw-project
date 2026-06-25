# Databricks notebook source
# ============================================
# NOTEBOOK 2 - SILVER LAYER
# Clean, fix data types, remove duplicates
# ============================================

from pyspark.sql.functions import to_date, col, trim, round

# Step 1: Read Bronze table
df_bronze = spark.table("`internship-proj1`.default.bronze_superstore")
print("Bronze row count:", df_bronze.count())

# Step 2: Fix data types
df_silver = df_bronze \
  .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
  .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd")) \
  .withColumn("sales", col("sales").cast("double")) \
  .withColumn("profit", col("profit").cast("double")) \
  .withColumn("discount", col("discount").cast("double")) \
  .withColumn("quantity", col("quantity").cast("integer")) \
  .withColumn("postal_code", col("postal_code").cast("string"))

# Step 3: Trim whitespace from all string columns
string_cols = ["order_id", "ship_mode", "customer_id", "customer_name",
               "segment", "country", "city", "state", "region",
               "product_id", "category", "sub_category", "product_name"]

for c in string_cols:
    df_silver = df_silver.withColumn(c, trim(col(c)))

# Step 4: Remove duplicates
before = df_silver.count()
df_silver = df_silver.dropDuplicates(["order_id", "product_id"])
after = df_silver.count()
print(f"Duplicates removed: {before - after} rows dropped")

# Step 5: Drop any rows where key fields are null
df_silver = df_silver.dropna(subset=["order_id", "customer_id", "product_id", "sales"])
print("Final silver row count:", df_silver.count())

# Step 6: Preview
df_silver.show(5)

# Step 7: Save as Silver Delta table
df_silver.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.silver_superstore")

print("✅ Silver layer done!")