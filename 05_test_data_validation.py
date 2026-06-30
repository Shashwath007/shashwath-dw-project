# Databricks notebook source
# ============================================
# NOTEBOOK 5 - TEST DATA GENERATION
# Inject bad rows to validate pipeline robustness
# ============================================

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

# Create test rows with intentional problems:
# 1. Null customer_id
# 2. Duplicate order_id + product_id (same as an existing row)
# 3. Bad date format
# 4. Extra whitespace in text fields
# 5. Negative/invalid sales value as string

test_data = [
    Row(row_id="99001", order_id="TEST-0001", order_date="2016-11-08", ship_date="2016-11-11",
        ship_mode="  Second Class  ", customer_id=None, customer_name="Test Null Customer",
        segment="Consumer", country="United States", city="Henderson", state="Kentucky",
        postal_code="42420", region="South", product_id="FUR-BO-10001798",
        category="Furniture", sub_category="Bookcases", product_name="Test Product Null Cust",
        sales="261.96", quantity="2", discount="0.0", profit="41.9136"),

    Row(row_id="99002", order_id="CA-2016-152156", order_date="2016-11-08", ship_date="2016-11-11",
        ship_mode="Second Class", customer_id="CG-12520", customer_name="Claire Gute",
        segment="Consumer", country="United States", city="Henderson", state="Kentucky",
        postal_code="42420", region="South", product_id="FUR-BO-10001798",
        category="Furniture", sub_category="Bookcases", product_name="Duplicate Row Test",
        sales="261.96", quantity="2", discount="0.0", profit="41.9136"),

    Row(row_id="99003", order_id="TEST-0003", order_date="08-11-2016", ship_date="11-11-2016",
        ship_mode="First Class", customer_id="TC-99003", customer_name="  Spacey Name  ",
        segment="  Corporate  ", country="United States", city="  Los Angeles  ", state="California",
        postal_code="90036", region="West", product_id="TEST-PROD-003",
        category="Office Supplies", sub_category="Labels", product_name="Test Bad Date Format",
        sales="14.62", quantity="2", discount="0.0", profit="6.8714"),

    Row(row_id="99004", order_id="TEST-0004", order_date="2016-10-11", ship_date="2016-10-18",
        ship_mode="Standard Class", customer_id="TC-99004", customer_name="Test Negative Sales",
        segment="Consumer", country="United States", city="Fort Lauderdale", state="Florida",
        postal_code="33311", region="South", product_id="TEST-PROD-004",
        category="Furniture", sub_category="Tables", product_name="Test Negative",
        sales="-50.00", quantity="5", discount="0.45", profit="-383.031"),

    Row(row_id="99005", order_id=None, order_date="2016-10-11", ship_date="2016-10-18",
        ship_mode="Standard Class", customer_id="TC-99005", customer_name="Test Null Order ID",
        segment="Consumer", country="United States", city="Fort Lauderdale", state="Florida",
        postal_code="33311", region="South", product_id="TEST-PROD-005",
        category="Office Supplies", sub_category="Storage", product_name="Test Null Order",
        sales="22.368", quantity="2", discount="0.2", profit="2.5164"),
]

# Create DataFrame matching Bronze schema
columns = ["row_id", "order_id", "order_date", "ship_date", "ship_mode",
           "customer_id", "customer_name", "segment", "country", "city",
           "state", "postal_code", "region", "product_id", "category",
           "sub_category", "product_name", "sales", "quantity", "discount", "profit"]

df_test = spark.createDataFrame(test_data, columns)

print("Test data created:")
df_test.show(truncate=False)
print(f"\nTotal test rows: {df_test.count()}")

# COMMAND ----------

# Check Bronze table schema
spark.table("`internship-proj1`.default.bronze_superstore").printSchema()

# COMMAND ----------

# ============================================
# STEP 1b - Rebuild test data with correct types
# Using try_to_date to handle bad formats safely
# ============================================

from pyspark.sql.functions import expr, col

# Convert our test DataFrame to match Bronze schema types
df_test_typed = df_test \
    .withColumn("row_id", col("row_id").cast("long")) \
    .withColumn("order_date", expr("try_to_date(order_date, 'yyyy-MM-dd')")) \
    .withColumn("ship_date", expr("try_to_date(ship_date, 'yyyy-MM-dd')")) \
    .withColumn("postal_code", col("postal_code").cast("long")) \
    .withColumn("sales", col("sales").cast("double")) \
    .withColumn("quantity", col("quantity").cast("long")) \
    .withColumn("discount", col("discount").cast("double")) \
    .withColumn("profit", col("profit").cast("double"))

print("Test data with correct types:")
df_test_typed.show(truncate=False)

# This row had a bad date format "08-11-2016" - let's see what happened to it
print("\nChecking the bad date format row (order_id = TEST-0003):")
df_test_typed.filter(col("order_id") == "TEST-0003").select("order_id", "order_date").show()

# COMMAND ----------

# ============================================
# STEP 2 - Inject test data into Bronze
# ============================================

# Read existing Bronze table
df_bronze_existing = spark.table("`internship-proj1`.default.bronze_superstore")

# Combine real Bronze data with typed test data
df_bronze_with_tests = df_bronze_existing.unionByName(df_test_typed)

# Save as a TEST bronze table (keeps your real Bronze table safe)
df_bronze_with_tests.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("`internship-proj1`.default.bronze_superstore_test")

print(f"✅ Test Bronze table created")
print(f"Original Bronze rows: {df_bronze_existing.count()}")
print(f"Bronze + test data: {df_bronze_with_tests.count()}")

# COMMAND ----------

# ============================================
# STEP 3 - Run test Bronze data through Silver logic
# ============================================

from pyspark.sql.functions import to_date, col, trim, round

# Read the TEST bronze table (with our 5 bad rows injected)
df_bronze_test = spark.table("`internship-proj1`.default.bronze_superstore_test")
print("Bronze test row count:", df_bronze_test.count())

# Apply the EXACT same Silver logic as your real notebook
df_silver_test = df_bronze_test \
  .withColumn("sales", col("sales").cast("double")) \
  .withColumn("profit", col("profit").cast("double")) \
  .withColumn("discount", col("discount").cast("double")) \
  .withColumn("quantity", col("quantity").cast("integer")) \
  .withColumn("postal_code", col("postal_code").cast("string"))

string_cols = ["order_id", "ship_mode", "customer_id", "customer_name",
               "segment", "country", "city", "state", "region",
               "product_id", "category", "sub_category", "product_name"]

for c in string_cols:
    df_silver_test = df_silver_test.withColumn(c, trim(col(c)))

# Remove duplicates
before = df_silver_test.count()
df_silver_test = df_silver_test.dropDuplicates(["order_id", "product_id"])
after = df_silver_test.count()
print(f"Duplicates removed: {before - after} rows dropped")

# Drop rows with null key fields
before_null_drop = df_silver_test.count()
df_silver_test = df_silver_test.dropna(subset=["order_id", "customer_id", "product_id", "sales"])
after_null_drop = df_silver_test.count()
print(f"Null key rows removed: {before_null_drop - after_null_drop}")

print(f"\nFinal Silver test row count: {df_silver_test.count()}")

# Check if our test rows survived or

# COMMAND ----------

# ============================================
# STEP 4 - Detailed validation results
# ============================================

print("=" * 60)
print("TEST RESULTS SUMMARY")
print("=" * 60)

# Test 1: Null customer_id (TEST-0001) - should be DROPPED
t1 = df_silver_test.filter(col("order_id") == "TEST-0001").count()
print(f"\n✅ TEST 1 - Null customer_id (TEST-0001)")
print(f"   Expected: Row dropped (null key field)")
print(f"   Result: {'PASS - dropped' if t1 == 0 else 'FAIL - still present'}")

# Test 2: Duplicate order_id+product_id (TEST-0002 = duplicate of CA-2016-152156)
t2 = df_silver_test.filter(col("product_name") == "Duplicate Row Test").count()
print(f"\n✅ TEST 2 - Duplicate row (same order_id + product_id)")
print(f"   Expected: One of the duplicates dropped")
print(f"   Result: {'PASS - duplicate removed' if t2 == 0 else 'FAIL - duplicate still present'}")

# Test 3: Bad date format (TEST-0003) - should have NULL order_date but row may still exist
t3 = df_silver_test.filter(col("order_id") == "TEST-0003").select("order_id", "order_date", "customer_name", "segment").collect()
print(f"\n✅ TEST 3 - Bad date format (TEST-0003)")
if len(t3) > 0:
    print(f"   Row survived with order_date = {t3[0]['order_date']}")
    print(f"   Customer name trimmed: '{t3[0]['customer_name']}'")
    print(f"   Segment trimmed: '{t3[0]['segment']}'")
    print(f"   Result: PARTIAL - row survived but has NULL date (not caught)")
else:
    print(f"   Result: PASS - row was dropped")

# Test 4: Negative sales (TEST-0004) - should this be allowed?
t4 = df_silver_test.filter(col("order_id") == "TEST-0004").select("order_id", "sales").collect()
print(f"\n✅ TEST 4 - Negative sales value (TEST-0004)")
if len(t4) > 0:
    print(f"   Row survived with sales = {t4[0]['sales']}")
    print(f"   Result: GAP FOUND - negative sales not validated/caught")
else:
    print(f"   Result: Row dropped")

# Test 5: Null order_id (TEST-0005) - should be DROPPED
t5 = df_silver_test.filter(col("customer_id") == "TC-99005").count()
print(f"\n✅ TEST 5 - Null order_id (TEST-0005)")
print(f"   Expected: Row dropped (null key field)")
print(f"   Result: {'PASS - dropped' if t5 == 0 else 'FAIL - still present'}")

print("\n" + "=" * 60)

# COMMAND ----------

# ============================================
# STEP 5 - Re-validate with the FIXED Silver logic
# ============================================

df_silver_test_v2 = df_bronze_test \
  .withColumn("sales", col("sales").cast("double")) \
  .withColumn("profit", col("profit").cast("double")) \
  .withColumn("discount", col("discount").cast("double")) \
  .withColumn("quantity", col("quantity").cast("integer")) \
  .withColumn("postal_code", col("postal_code").cast("string"))

for c in string_cols:
    df_silver_test_v2 = df_silver_test_v2.withColumn(c, trim(col(c)))

df_silver_test_v2 = df_silver_test_v2.dropDuplicates(["order_id", "product_id"])
df_silver_test_v2 = df_silver_test_v2.dropna(subset=["order_id", "customer_id", "product_id", "sales"])

# NEW: the fix we added
df_silver_test_v2 = df_silver_test_v2.dropna(subset=["order_date"])
df_silver_test_v2 = df_silver_test_v2.filter(col("sales") >= 0)

print(f"Final row count after fix: {df_silver_test_v2.count()}")

# Re-check TEST-0003 (bad date) and TEST-0004 (negative sales)
t3_check = df_silver_test_v2.filter(col("order_id") == "TEST-0003").count()
t4_check = df_silver_test_v2.filter(col("order_id") == "TEST-0004").count()

print(f"\nTEST 3 (bad date) - now {'PASS - dropped' if t3_check == 0 else 'still present'}")
print(f"TEST 4 (negative sales) - now {'PASS - dropped' if t4_check == 0 else 'still present'}")

# COMMAND ----------

# ============================================
# FINAL CHECK - Run all 5 tests in one go
# ============================================

print("=" * 60)
print("FINAL VALIDATION CHECK — Silver Layer Data Quality")
print("=" * 60)

results = []

# Test 1: Null customer_id should be dropped
t1 = df_silver_test_v2.filter(col("order_id") == "TEST-0001").count()
results.append(("Null customer_id", t1 == 0))

# Test 2: Duplicate row should be removed
t2 = df_silver_test_v2.filter(col("product_name") == "Duplicate Row Test").count()
results.append(("Duplicate order_id + product_id", t2 == 0))

# Test 3: Bad date format should be dropped
t3 = df_silver_test_v2.filter(col("order_id") == "TEST-0003").count()
results.append(("Bad date format", t3 == 0))

# Test 4: Negative sales should be dropped
t4 = df_silver_test_v2.filter(col("order_id") == "TEST-0004").count()
results.append(("Negative sales value", t4 == 0))

# Test 5: Null order_id should be dropped
t5 = df_silver_test_v2.filter(col("customer_id") == "TC-99005").count()
results.append(("Null order_id", t5 == 0))

# Print results
passed = 0
for name, ok in results:
    status = "✅ PASS" if ok else "❌ FAIL"
    if ok:
        passed += 1
    print(f"{status}  —  {name}")

print("=" * 60)
print(f"RESULT: {passed}/5 tests passed")
if passed == 5:
    print("🏆 All data quality checks passed! Silver layer is validated.")
else:
    print("⚠️ Some checks failed — review Silver layer logic.")
print("=" * 60)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS `internship-proj1`.default.bronze_superstore_test")

# COMMAND ----------

