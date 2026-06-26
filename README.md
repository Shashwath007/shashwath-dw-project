# Superstore Data Warehouse Project

![Architecture](architecture_diagram.png)

## Project Overview

An end-to-end data warehouse built during my data engineering internship using the **Medallion Architecture** (Bronze → Silver → Gold) on **Azure** and **Databricks**, with a **Power BI dashboard** for business insights.

- **Dataset:** Superstore Sales Dataset (Kaggle) — 9,994 rows, 21 columns
- **Dataset Link:** [Superstore Sales Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
- **Tech Stack:** Azure Data Lake Gen2, Azure SQL DB, Databricks, PySpark, Power BI, GitHub
- **Live Dashboard:** [Click here to view](https://app.powerbi.com/links/eXs8qx-Xeq?ctid=51697115-1ecd-42b5-b509-2d62c3919f76&pbi_source=linkShare)

---

## Architecture

```
Superstore CSV
      |
      v  upload
ADLS Gen2 (Bronze container)
      |
      v  PySpark
Databricks Silver Layer  ──── Cleaned, deduped, typed
      |
      v  PySpark
Databricks Gold Layer  ────── Star schema (Fact + 4 Dims)
      |                 |
      v  direct         v  JDBC (blocked — Serverless limitation)
Power BI Dashboard    Azure SQL DB
```

---

## Repository Structure

```
shashwath-dw-project/
|
├── 01_bronze.py              # Ingest raw CSV → Bronze Delta table
├── 02_silver.py              # Clean, dedupe, fix types → Silver Delta table
├── 03_gold.py                # Build Star Schema → Gold Delta tables + SCD Type 1
├── architecture_diagram.png  # Full architecture diagram
├── screenshots/              # Power BI dashboard screenshots
|   ├── page1_executive_summary.png
|   ├── page2_sales_analysis.png
|   ├── page3_profit_analysis.png
|   ├── page4_customer_analysis.png
|   └── page5_product_analysis.png
└── README.md
```

---

## Notebooks

### Notebook 1 — Bronze Layer (`01_bronze.py`)
- Reads raw Superstore CSV uploaded to Databricks
- Renames columns (removes spaces, lowercases)
- Saves as a Delta table: `bronze_superstore`
- **Output:** 9,994 rows, 21 columns

### Notebook 2 — Silver Layer (`02_silver.py`)
- Reads Bronze Delta table
- Fixes data types (dates, doubles, integers)
- Trims whitespace from all string columns
- Removes duplicates based on `order_id` + `product_id`
- Drops rows with null values in key fields
- **Output:** 9,986 rows (8 duplicates removed)

### Notebook 3 — Gold Layer (`03_gold.py`)
- Builds a Star Schema with 1 Fact table and 4 Dimension tables
- Applies SCD Type 1 on `DimCustomer` (overwrites changed records)

**Star Schema:**
```
                    ┌─────────────┐
                    │  DimDate    │
                    │  1,237 rows │
                    └──────┬──────┘
                           |
┌──────────────┐    ┌──────v──────┐    ┌───────────────┐
│ DimCustomer  │────│ FactOrders  │────│  DimProduct   │
│  793 rows    │    │ 10,322 rows │    │  1,894 rows   │
└──────────────┘    └──────┬──────┘    └───────────────┘
                           |
                    ┌──────v──────┐
                    │  DimRegion  │
                    │  604 rows   │
                    └─────────────┘
```

| Table | Rows | Key Columns |
|---|---|---|
| `gold_fact_orders` | 10,322 | order_id, date_key, customer_key, product_key, region_key, sales, profit, quantity, discount, ship_mode |
| `gold_dim_date` | 1,237 | date_key, year, month, quarter, day, month_name |
| `gold_dim_customer` | 793 | customer_key, customer_id, customer_name, segment |
| `gold_dim_product` | 1,894 | product_key, product_id, product_name, category, sub_category |
| `gold_dim_region` | 604 | region_key, region, state, city |

---

## Power BI Dashboard

**Live Dashboard:** [Click here to view](https://app.powerbi.com/links/eXs8qx-Xeq?ctid=51697115-1ecd-42b5-b509-2d62c3919f76&pbi_source=linkShare)

The dashboard has 5 pages with 14 DAX measures covering sales, profit, customer and product analysis.

---

### Page 1 — Executive Summary

![Executive Summary](screenshots/page1_executive_summary.png)

This page gives a high-level overview of the entire business. Five KPI cards at the top show Total Sales ($2.39M), Total Profit ($299.22K), Total Orders (5K), Profit Margin % (12.50%), and Average Order Value ($477.73). Below the cards, a horizontal bar chart shows sales by region with West leading at $500K+, followed by East, Central, and South. A donut chart breaks down sales by category showing Technology at 37.33%, Office Supplies at 30.76%, and Furniture at 31.92%. A monthly bar chart on the right shows sales trend across the year with a clear peak in November and December.

---

### Page 2 — Sales Analysis

![Sales Analysis](screenshots/page2_sales_analysis.png)

This page digs into revenue breakdown across different dimensions. Four KPI cards show Total Sales ($2.39M), Total Quantity (39K), Avg Order Value ($477.73), and Avg Discount % (15.59%). The top left bar chart shows sales by sub-category with Phones leading at $0.3M+ followed by Storage and Tables. The top right pie chart shows sales by ship mode with Standard Class dominating at 59.29% ($1.42M), followed by Second Class at 19.68%, First Class at 15.25%, and Same Day at 5.78%. The bottom left column chart shows yearly sales trend growing steadily from 2014 to 2017. The bottom right bar chart shows California as the top state by sales, followed by New York and Texas.

---

### Page 3 — Profit Analysis

![Profit Analysis](screenshots/page3_profit_analysis.png)

This page focuses on profitability. Four KPI cards show Total Profit ($299.22K) in green, Profit Margin % (12.50%) in green, Total Loss (-$161.32K) in red, and Profitable Orders (8K) in blue. The top left bar chart shows Copiers as the most profitable sub-category ($55K+), followed by Accessories and Phones. The top right bar chart shows the West region leads in profit ($100K+) followed by East, South, and Central. The bottom left column chart shows yearly profit growing consistently from 2014 to 2017. The bottom right bar chart shows Technology ($150K+) and Office Supplies ($130K+) as the most profitable categories while Furniture has very low profit.

---

### Page 4 — Customer Analysis

![Customer Analysis](screenshots/page4_customer_analysis.png)

This page analyses customer behaviour and segments. Four KPI cards show 793 Total Customers, 781 Repeat Customers (strong retention), Avg Sales per Customer ($3.02K), and Total Orders (5K). The top left bar chart shows the Consumer segment generates the highest sales ($1M+), followed by Corporate and Home Office. The top right bar chart shows Sean Miller as the top customer at $25K+, followed by Tamara Chand, Greg Tran, Raymond Buch, and Adrian Barton. The bottom left column chart shows total orders growing every year from 2014 to 2017. The bottom right pie chart shows profit by segment with Consumer leading at 47.14% ($141.06K), Corporate at 31.48% ($94.21K), and Home Office at 21.37% ($63.96K).

---

### Page 5 — Product Analysis

![Product Analysis](screenshots/page5_product_analysis.png)

This page covers product performance. Four KPI cards show 2K Total Products, $2.39M Total Sales, 15.59% Avg Discount, and 39K Total Quantity. The top left bar chart shows the top 5 products by sales with Canon imageCLASS 2200 leading at $60K+, followed by Fellowes PB500, Cisco TelePresence, HON 5400 Series, and GBC DocuBind. The top right donut chart shows Technology at 37.33%, Office Supplies at 30.76%, and Furniture at 31.92% of total sales. The bottom left bar chart shows Binders as the highest quantity sold at 6K+ units followed by Paper and Furnishings. The bottom right bar chart shows Copiers as the most profitable sub-category ($55K+) while the left side shows sub-categories with negative profit.

---

## Orchestration

**Databricks Workflow:** `superstore-daily-pipeline`

| Task | Notebook | Depends On |
|---|---|---|
| `bronze_ingestion` | `01_bronze` | — |
| `silver_cleaning` | `02_silver` | bronze_ingestion |
| `gold_star_schema` | `03_gold` | silver_cleaning |

- **Schedule:** Daily at 6:00 AM
- **Alert:** Email notification on failure
- **Last run:** Succeeded in 1 minute 7 seconds

---

## DAX Measures

```
Total Sales = SUM(gold_fact_orders[sales])
Total Profit = SUM(gold_fact_orders[profit])
Total Orders = DISTINCTCOUNT(gold_fact_orders[order_id])
Profit Margin % = DIVIDE([Total Profit], [Total Sales], 0) * 100
Avg Order Value = DIVIDE([Total Sales], [Total Orders], 0)
Total Quantity = SUM(gold_fact_orders[quantity])
Avg Discount % = AVERAGE(gold_fact_orders[discount]) * 100
Total Loss = SUMX(FILTER(gold_fact_orders, gold_fact_orders[profit] < 0), gold_fact_orders[profit])
Profitable Orders = COUNTROWS(FILTER(gold_fact_orders, gold_fact_orders[profit] > 0))
Loss Orders = COUNTROWS(FILTER(gold_fact_orders, gold_fact_orders[profit] < 0))
Total Customers = DISTINCTCOUNT(gold_fact_orders[customer_key])
Avg Sales per Customer = DIVIDE([Total Sales], [Total Customers], 0)
Repeat Customers = COUNTROWS(FILTER(VALUES(gold_fact_orders[customer_key]), CALCULATE(DISTINCTCOUNT(gold_fact_orders[order_id])) > 1))
Total Products = DISTINCTCOUNT(gold_dim_product[product_id])
```

---

## Key Business Insights

| Question | Answer |
|---|---|
| Top region by sales? | West — $725K |
| Most profitable sub-category? | Copiers — $55K profit |
| Least profitable sub-category? | Tables — loss making |
| Top customer? | Sean Miller — $25K |
| Top product? | Canon imageCLASS 2200 — $61K |
| Best segment by sales? | Consumer — highest sales and profit |
| Sales trend? | Growing year over year, peak in Nov/Dec |
| Most used ship mode? | Standard Class — 59.29% of orders |

---

## Setup Steps

**Prerequisites**
- Azure Free Account
- Databricks Community Edition
- Power BI Desktop
- GitHub account

**Steps to Reproduce**

1. Download Superstore dataset from Kaggle
2. Upload CSV to Databricks Catalog under `internship-proj1.default`
3. Run notebooks in order: `01_bronze` → `02_silver` → `03_gold`
4. Open Power BI Desktop → Get Data → Azure Databricks
5. Connect using Server hostname and HTTP path from Databricks SQL Warehouse
6. Load all 5 Gold tables
7. Set up relationships between tables in Model view
8. Create DAX measures and build report pages

---

## Author

**Shashwath**
Data Engineering Intern
GitHub: [shashwath-dw-project](https://github.com/Shashwath007/shashwath-dw-project)
