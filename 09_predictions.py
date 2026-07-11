# Databricks notebook source
# ============================================
# NOTEBOOK 9 - PREDICTIONS & FORECASTING
# Uses historical Gold data + live streaming
# ============================================

# Cell 1: Setup
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, month, year,
    date_format, lit, round as spark_round, max as spark_max
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas as pd
from datetime import datetime, timedelta

# Read Gold tables
fact = spark.table("`internship-proj1`.default.gold_fact_orders")
dim_date = spark.table("`internship-proj1`.default.gold_dim_date")
dim_product = spark.table("`internship-proj1`.default.gold_dim_product")
dim_region = spark.table("`internship-proj1`.default.gold_dim_region")

# Read live streaming gold (if available)
try:
    stream_gold = spark.table("`internship-proj1`.default.streaming_orders_gold")
    has_stream = True
    print("✅ Streaming data loaded")
except:
    has_stream = False
    print("⚠️ No streaming data — using batch only")

print("✅ All tables loaded")
print(f"Fact rows: {fact.count()}")

# COMMAND ----------

# ============================================
# Cell 2: Build monthly sales trend
# ============================================

# Join fact with date dimension
fact_with_date = fact.join(
    dim_date.select("date_key", "year", "month"),
    on="date_key", how="left"
)

# Aggregate sales by year and month
monthly_sales = fact_with_date.groupBy("year", "month") \
    .agg(
        spark_round(spark_sum("sales"), 2).alias("total_sales"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        count("order_id").alias("total_orders")
    ) \
    .withColumn("month_index", (col("year") - 2015) * 12 + col("month")) \
    .orderBy("year", "month")

# Add streaming data if available
if has_stream:
    stream_monthly = stream_gold.groupBy() \
        .agg(
            spark_round(spark_sum("total_sales"), 2).alias("total_sales"),
            spark_round(spark_sum("total_profit"), 2).alias("total_profit"),
            spark_round(spark_sum("total_orders"), 2).alias("total_orders")
        ) \
        .withColumn("year", lit(2026)) \
        .withColumn("month", lit(datetime.now().month)) \
        .withColumn("month_index", lit((2026 - 2015) * 12 + datetime.now().month))
    
    monthly_sales = monthly_sales.unionByName(stream_monthly)
    print("✅ Streaming data merged into monthly trend")

monthly_sales.show(20)
print(f"Total months of data: {monthly_sales.count()}")

# COMMAND ----------

# ============================================
# Cell 3: Train ML model and predict next 3 months
# ============================================

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import when

# Prepare features for ML
assembler = VectorAssembler(
    inputCols=["month_index"],
    outputCol="features"
)

ml_data = assembler.transform(monthly_sales) \
    .select("month_index", "features", "total_sales", "total_profit", "total_orders")

# Train Sales model
lr_sales = LinearRegression(featuresCol="features", labelCol="total_sales")
model_sales = lr_sales.fit(ml_data)

# Train Profit model
lr_profit = LinearRegression(featuresCol="features", labelCol="total_profit")
model_profit = lr_profit.fit(ml_data)

# Train Orders model
lr_orders = LinearRegression(featuresCol="features", labelCol="total_orders")
model_orders = lr_orders.fit(ml_data)

print(f"Sales model R²: {round(model_sales.summary.r2, 3)}")
print(f"Profit model R²: {round(model_profit.summary.r2, 3)}")
print(f"Orders model R²: {round(model_orders.summary.r2, 3)}")

# Predict next 3 months
current_max_index = monthly_sales.agg(spark_max("month_index")).collect()[0][0]
future_months = [(current_max_index + i,) for i in range(1, 4)]

future_df = spark.createDataFrame(future_months, ["month_index"])
future_df = assembler.transform(future_df)

# Get predictions
sales_preds = model_sales.transform(future_df).select("month_index", col("prediction").alias("predicted_sales"))
profit_preds = model_profit.transform(future_df).select("month_index", col("prediction").alias("predicted_profit"))
orders_preds = model_orders.transform(future_df).select("month_index", col("prediction").alias("predicted_orders"))

# Join all predictions
predictions = sales_preds \
    .join(profit_preds, on="month_index") \
    .join(orders_preds, on="month_index") \
    .withColumn("predicted_sales", spark_round(col("predicted_sales"), 2)) \
    .withColumn("predicted_profit", spark_round(col("predicted_profit"), 2)) \
    .withColumn("predicted_orders", spark_round(col("predicted_orders"), 0).cast("integer")) \
    .withColumn("year", lit(2026)) \
    .withColumn("month", (col("month_index") - (2026 - 2015) * 12).cast("integer")) \
    .withColumn("month_name",
        when(col("month") == 1, "Jan 2026")
        .when(col("month") == 2, "Feb 2026")
        .when(col("month") == 3, "Mar 2026")
        .when(col("month") == 4, "Apr 2026")
        .when(col("month") == 5, "May 2026")
        .when(col("month") == 6, "Jun 2026")
        .when(col("month") == 7, "Jul 2026")
        .when(col("month") == 8, "Aug 2026")
        .when(col("month") == 9, "Sep 2026")
        .when(col("month") == 10, "Oct 2026")
        .when(col("month") == 11, "Nov 2026")
        .when(col("month") == 12, "Dec 2026")
    ) \
    .withColumn("data_type", lit("Forecast")) \
    .orderBy("month_index")

predictions.show()

# COMMAND ----------

# ============================================
# Cell 4: Category + Region predictions
# Save all predictions to Gold table
# ============================================

from pyspark.sql.functions import concat, lit as lit_fn

# ── Category level profit prediction ──
fact_with_product = fact.join(
    dim_product.select("product_key", "category"),
    on="product_key", how="left"
).join(
    dim_date.select("date_key", "year", "month"),
    on="date_key", how="left"
)

category_monthly = fact_with_product.groupBy("category", "year", "month") \
    .agg(
        spark_round(spark_sum("sales"), 2).alias("total_sales"),
        spark_round(spark_sum("profit"), 2).alias("total_profit")
    ) \
    .withColumn("month_index", (col("year") - 2015) * 12 + col("month"))

# Train one model per category
categories = ["Technology", "Furniture", "Office Supplies"]
category_preds = []

for cat in categories:
    cat_data = category_monthly.filter(col("category") == cat)
    cat_ml = assembler.transform(cat_data).select("month_index", "features", "total_sales", "total_profit")
    
    # Sales model per category
    cat_model = LinearRegression(featuresCol="features", labelCol="total_sales").fit(cat_ml)
    cat_future = assembler.transform(future_df.select("month_index"))
    cat_pred = cat_model.transform(cat_future) \
        .select("month_index", col("prediction").alias("predicted_sales")) \
        .withColumn("category", lit(cat))
    category_preds.append(cat_pred)
    print(f"✅ {cat} model trained — R²: {round(cat_model.summary.r2, 3)}")

# Combine category predictions
from functools import reduce
cat_predictions = reduce(lambda a, b: a.unionByName(b), category_preds) \
    .withColumn("predicted_sales", spark_round(col("predicted_sales"), 2)) \
    .withColumn("month_name",
        when(col("month_index") == current_max_index + 1, "Aug 2026")
        .when(col("month_index") == current_max_index + 2, "Sep 2026")
        .when(col("month_index") == current_max_index + 3, "Oct 2026")
    ) \
    .withColumn("data_type", lit("Category Forecast"))

print("\nCategory predictions:")
cat_predictions.show()

# ── Region level sales prediction ──
fact_with_region = fact.join(
    dim_region.select("region_key", "region"),
    on="region_key", how="left"
).join(
    dim_date.select("date_key", "year", "month"),
    on="date_key", how="left"
)

region_monthly = fact_with_region.groupBy("region", "year", "month") \
    .agg(spark_round(spark_sum("sales"), 2).alias("total_sales")) \
    .withColumn("month_index", (col("year") - 2015) * 12 + col("month"))

regions = ["West", "East", "Central", "South"]
region_preds = []

for reg in regions:
    reg_data = region_monthly.filter(col("region") == reg)
    reg_ml = assembler.transform(reg_data).select("month_index", "features", "total_sales")
    reg_model = LinearRegression(featuresCol="features", labelCol="total_sales").fit(reg_ml)
    reg_future = assembler.transform(future_df.select("month_index"))
    reg_pred = reg_model.transform(reg_future) \
        .select("month_index", col("prediction").alias("predicted_sales")) \
        .withColumn("region", lit(reg))
    region_preds.append(reg_pred)
    print(f"✅ {reg} model trained — R²: {round(reg_model.summary.r2, 3)}")

reg_predictions = reduce(lambda a, b: a.unionByName(b), region_preds) \
    .withColumn("predicted_sales", spark_round(col("predicted_sales"), 2)) \
    .withColumn("month_name",
        when(col("month_index") == current_max_index + 1, "Aug 2026")
        .when(col("month_index") == current_max_index + 2, "Sep 2026")
        .when(col("month_index") == current_max_index + 3, "Oct 2026")
    ) \
    .withColumn("data_type", lit("Region Forecast"))

print("\nRegion predictions:")
reg_predictions.show()

# ── Save overall predictions to Delta ──
predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`internship-proj1`.default.gold_predictions")

# ── Save category predictions ──
cat_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`internship-proj1`.default.gold_predictions_category")

# ── Save region predictions ──
reg_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`internship-proj1`.default.gold_predictions_region")

print("\n🏆 All predictions saved!")
print("Tables created:")
print("  - gold_predictions (overall)")
print("  - gold_predictions_category")
print("  - gold_predictions_region")

# COMMAND ----------

