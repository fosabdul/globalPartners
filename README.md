
GlobalPartners Data Engineering & Analytics Pipeline

Project Overview

This project builds an end-to-end data engineering and analytics pipeline for GlobalPartners transactional data.

Raw order data from SQL Server is ingested, transformed with AWS Glue, stored in S3 using a Bronze/Silver/Gold architecture, and analyzed through Athena and a Streamlit dashboard.

The goal is to demonstrate how raw operational data can be transformed into business-ready analytics datasets.

🏗 Architecture

SQL Server → AWS Glue → Amazon S3 → Glue Data Catalog → Athena → Streamlit

Glue performs ETL and modeling

S3 stores layered datasets

Athena enables SQL analytics

Streamlit provides visualization

🧱 Data Layers
Bronze

Raw ingestion from SQL Server.

Contains original order_items data and date dimension.

Silver

Cleaned and standardized dataset.

Includes:

selected fields

normalized schema

reusable analytics foundation

Gold

Business-ready analytics datasets:

Category revenue KPIs

Order metrics

Daily CLV (proxy)

Retention dataset

Cohort dataset

📊 Analytics Models
Category KPIs

Revenue and volume by item category.

Used for product performance analysis.

Order Metrics

Order-level revenue and item counts.

Used for operational monitoring.

Daily CLV (Proxy)

Revenue aggregated by order and date.

⚠️ The source dataset contained inconsistent user identifiers, so customer lifetime value was modeled at the order level as a proxy.

Retention

Order activity over time by restaurant.

Used to measure repeat engagement.

Cohort Analysis

Restaurants grouped by first activity month to track lifecycle trends.

📈 Dashboard

A Streamlit dashboard queries Gold datasets via Athena and visualizes:

Revenue by category

Revenue trends

Retention activity

Cohort growth

The dashboard demonstrates how analytics datasets power business reporting.

🧰 Tech Stack

AWS Glue

Amazon S3

Glue Data Catalog

Amazon Athena

PySpark

SQL Server

Streamlit

Python

⚙️ Key Data Engineering Concepts Demonstrated

Layered data architecture (Bronze/Silver/Gold)

JDBC ingestion

Schema validation and data quality checks

Handling missing identifiers

Time dimension recovery from event IDs

Analytical data modeling

Cloud-native querying with Athena

Dashboard integration