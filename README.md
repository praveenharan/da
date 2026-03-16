# Clinical Financial Decision Support Engine

## Overview
This project serves as a mock **Financial Decision Support system** designed for an academic medical center environment. It simulates the end-to-end process of ingesting raw clinical billing and expense data, performing complex financial transformations, and surfacing actionable insights for senior management.

> **Note:** All data within this repository is synthetic and generated for demonstration purposes. It contains no Protected Health Information (PHI) or proprietary financial records.

## Objective
To provide automated, high-precision financial reporting that allows clinical departments to identify **unfavorable trends** and perform **root-cause analysis** on billing operations.

## Pipeline Architecture


The pipeline follows a robust **Medallion Architecture** to ensure data integrity and auditability:

* **Ingestion:** Raw billing and expense files are ingested into ADLS Gen2.
* **Transformation Layers:**
    * **Bronze:** Raw landing zone for source data.
    * **Silver:** Data cleaning, deduplication, and schema enforcement.
    * **Gold:** Aggregated financial metrics and KPI modeling (DAX-ready).
* **Visualization:** Power BI dashboards featuring "Drill-Down" capabilities and "What-If" parameters for clinical revenue forecasting.

## Key Technical Features
* **Medallion Architecture:** Implemented a robust data pipeline using **Azure Data Factory** and **Delta Lake**.
* **Performance Optimization:** Utilized **Liquid Clustering** and **Z-Ordering** on partition keys (e.g., Department ID, Fiscal Date) to optimize large-scale financial model queries.
* **Complex Financial Modeling:** Built a normalized **Star Schema** (Fact/Dimension tables) designed for high-performance financial variance analysis.
* **Automated Quality Assurance:** Developed **PySpark/SQL** scripts for master file validation, ensuring consistency across clinical billing systems and cost centers.
* **Real-Time Anomaly Detection:** Leveraged **KQL (Kusto Query Language)** to identify sudden spikes in billing denials and operational cost outliers, enabling proactive management intervention.

## Repository Contents
* `/notebooks/`: PySpark/SQL transformations and data quality checks.
* `/queries/`: KQL scripts for real-time anomaly detection.
* `/data/`: Sample synthetic schema structures.
* `/docs/`: Architecture diagram and data dictionary.

## Alignment with Financial Analyst Competencies
* **Decision Support:** Provides senior management with drill-down reporting to pinpoint root causes of financial variances.
* **Risk Identification:** Proactively alerts clinical departments to unfavorable trends using real-time data monitoring.
* **Data Integrity:** Implements strict QA processes over master financial files.
