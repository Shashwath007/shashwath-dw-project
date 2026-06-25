# Databricks notebook source
# ============================================
# NOTEBOOK 1 - BRONZE LAYER
# ============================================

# Step 1: Read the raw table
df_raw = spark.table("`internship-proj1`.default.bronze_superstor_raw")

# Step 2: Rename columns - replace spaces with underscores
df_clean_cols = df_raw.toDF(
  "row_id", "order_id", "order_date", "ship_date", "ship_mode",
  "customer_id", "customer_name", "segment", "country", "city",
  "state", "postal_code", "region", "product_id", "category",
  "sub_category", "product_name", "sales", "quantity", "discount", "profit"
)

# Step 3: Check it
print("Row count:", df_clean_cols.count())
print("Columns:", df_clean_cols.columns)
df_clean_cols.show(5)

# Step 4: Save as Bronze Delta table
df_clean_cols.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("`internship-proj1`.default.bronze_superstore")

print("✅ Bronze layer done!")