# GlobalPartners Data Engineering & Analytics Pipeline

> End-to-end cloud data pipeline ingesting transactional order data from SQL Server, transforming it through a Bronze/Silver/Gold lakehouse architecture on AWS, and surfacing business insights through an interactive Streamlit dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Layers](#data-layers)
- [Pipeline Orchestration](#pipeline-orchestration)
- [Analytics Models](#analytics-models)
- [Dashboard](#dashboard)
- [CI/CD Pipeline](#cicd-pipeline)
- [Tech Stack](#tech-stack)
- [Data Quality Notes](#data-quality-notes)
- [Key Concepts Demonstrated](#key-concepts-demonstrated)

---

## Project Overview

GlobalPartners operates a restaurant ordering platform generating high-volume transactional data across hundreds of locations. This project delivers a production-grade data engineering pipeline that:

- Ingests raw order, item, and options data from a SQL Server source database
- Transforms and models data into analytics-ready Gold tables using AWS Glue and PySpark
- Stores all datasets in Amazon S3 following a layered Medallion architecture
- Enables SQL-based analytics via Amazon Athena and the AWS Glue Data Catalog
- Visualizes business KPIs through a multi-section Streamlit executive dashboard
- Automates daily pipeline execution through AWS Glue Workflows
- Deploys infrastructure updates automatically via GitHub Actions CI/CD

The project demonstrates how raw operational data can be reliably transformed into business-ready analytics at scale using modern cloud-native tooling.

---

## Architecture

```
SQL Server (Azure)
       │
       ▼
  AWS Glue ETL Jobs (PySpark)
       │
       ▼
Amazon S3 — Medallion Architecture
  ├── bronze/   ← Raw ingestion
  ├── silver/   ← Cleaned & standardized
  └── gold/     ← Business-ready models
       │
       ▼
AWS Glue Data Catalog + Crawlers
       │
       ▼
Amazon Athena (SQL query layer)
       │
       ▼
Streamlit Dashboard (Executive Analytics)
       │
       ▼
GitHub Actions (CI/CD — auto-deploy on push)
```

---

## Data Layers

### 🥉 Bronze — Raw Ingestion
Direct copy of source data from SQL Server with no transformations applied. Preserves all original columns and data types as the source of truth.

| Table | Description |
|---|---|
| `bronze/order_items/` | Raw order line items with restaurant, category, price, quantity |
| `bronze/order_item_options/` | Raw item-level add-ons, modifiers, and discounts |
| `bronze/date_dim/` | Generated date dimension covering 2019–2025 |

### 🥈 Silver — Cleaned & Standardized
Schema-normalized datasets with consistent column naming, correct data types, and reusable structure. Serves as a stable foundation for Gold modeling.

### 🥇 Gold — Business-Ready Analytics Models
Aggregated, business-aligned datasets optimized for reporting and dashboard queries.

| Table | Description |
|---|---|
| `gold/order_metrics/` | Order-level revenue and item counts |
| `gold/daily_clv/` | Daily order revenue for CLV trend analysis |
| `gold/category_revenue/` | Revenue and volume aggregated by menu category |
| `gold/retention/` | Order activity over time by restaurant |
| `gold/cohort/` | Restaurants grouped by first activity month |

---

## Pipeline Orchestration

Daily batch execution is managed through **AWS Glue Workflows** with event-based triggers ensuring jobs run in dependency order:

```
trigger_daily_start (Scheduled — 6:00 AM UTC)
    └── gp_mysql_to_s3_raw_order_items
            └── trigger_after_order_items
                    ├── gp_mysql_to_s3_raw_order_item_options
                    └── gp_mysql_to_s3_raw_date_dim
                            └── trigger_after_ingestion (ALL — waits for both)
                                    └── gp_transform_curated_analytics
                                            └── trigger_after_transform
                                                    └── gp_silver_order_items_crawler
```

**Trigger types used:**
- `SCHEDULED` — time-based daily start
- `CONDITIONAL (ANY)` — fires after any watched job succeeds (parallel fan-out)
- `CONDITIONAL (ALL)` — fires only after all watched jobs succeed (dependency gate)

---

## Analytics Models

### Customer Lifetime Value (CLV)
Order-level CLV combining base item revenue and option add-on/discount amounts from `order_item_options`. Segmented into High, Medium, and Low tiers using percentile-based thresholds.

### RFM Segmentation
Restaurant-level Recency, Frequency, and Monetary analysis producing four segments: VIP, Active, New Customer, and Churn Risk. Percentile-based scoring applied across all three dimensions.

### Churn Risk Indicators
Days since last order and average order gap per restaurant. Restaurants flagged as At Risk (>45 days inactive), Watch (>20 days), or Active.

### Sales Trends & Seasonality
Daily and monthly revenue trends joined with the date dimension for weekday/weekend breakdowns and seasonal pattern analysis.

### Loyalty Program Impact
High vs Low frequency restaurant segmentation using order volume as a proxy for loyalty behavior. Includes spend comparison and category preference analysis.

### Location Performance
Restaurant ranking by total revenue with cohort month context. Tracks active days, total orders, and average order value per location.

### Pricing & Discount Effectiveness
Discounted vs full-price order comparison using `order_item_options` where `option_price < 0` indicates a discount. Shows gross revenue, net revenue after discounts, and total discount amount given.

---

## Dashboard

The Streamlit dashboard connects to Athena via `boto3` and executes SQL queries against Gold tables to surface real-time business metrics across seven sections:

| Section | Key Metrics |
|---|---|
| CLV | Total orders, avg CLV, high-value order count |
| RFM Segmentation | VIP vs churn risk restaurant breakdown |
| Churn Risk | At-risk locations, days since last order |
| Sales Trends | Daily/monthly revenue, category performance |
| Loyalty Impact | High vs low frequency spend comparison |
| Location Performance | Revenue ranking, avg order value by location |
| Pricing & Discounts | Discounted vs full-price revenue comparison |

---

## CI/CD Pipeline

Every push to the `main` branch automatically triggers a GitHub Actions workflow that:

1. Checks out the latest code
2. Authenticates with AWS using stored GitHub Secrets
3. Uploads all Glue job scripts to the S3 scripts bucket
4. Uploads the latest dashboard code to S3

```yaml
# Triggered on every push to main
on:
  push:
    branches:
      - main
```

**Required GitHub Secrets:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source Database | SQL Server (Azure RDS) |
| Ingestion & Transformation | AWS Glue 5.0 (PySpark) |
| Storage | Amazon S3 (Medallion Architecture) |
| Schema Registry | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Orchestration | AWS Glue Workflows |
| Dashboard | Streamlit + Boto3 |
| CI/CD | GitHub Actions |
| Language | Python 3.11 |

---

## Data Quality Notes

During pipeline development, the following data quality issues were identified and documented:

| Field | Issue | Resolution |
|---|---|---|
| `userid` | Entirely NULL in source — not populated by application | CLV modeled at order level as proxy |
| `isloyalty` | Entirely NULL in source | Loyalty analysis uses order frequency as proxy |
| `order_item_options` | Table empty in source system | Sample data generated to demonstrate pipeline and discount analysis |
| `orderid` | Stored as ISO 8601 timestamp string | Parsed using `regexp_replace` + `CAST AS TIMESTAMP` in Athena |

---

## Key Concepts Demonstrated

- Medallion lakehouse architecture (Bronze / Silver / Gold)
- JDBC ingestion from SQL Server using PySpark
- Schema normalization and column standardization
- Handling NULL identifiers and missing dimensional data
- Timestamp recovery from event ID strings
- Percentile-based customer segmentation (RFM, CLV)
- Discount detection using negative option prices
- Cohort analysis for restaurant lifecycle tracking
- Cloud-native SQL analytics with Athena
- Event-driven pipeline orchestration with dependency gates
- Automated CI/CD deployment via GitHub Actions