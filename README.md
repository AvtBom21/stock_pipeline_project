# Stock Market Data Pipeline Project

**A Real-World Batch Data Pipeline for Ingesting, Processing, and Warehousing Stock Market Data Using Python, Yahoo Finance API, MySQL, and Airflow-like orchestration.**

---

## Overview

This project implements a **production-grade data pipeline** for collecting and managing historical and daily stock data using open data sources (Yahoo Finance). The pipeline includes:

- Data ingestion (historical & incremental)
- Data cleaning and feature engineering (technical indicators)
- Local backup and versioning
- Data loading to MySQL (data warehouse simulation)
- Logging, modular design, and configuration management
- Ready for orchestration using Airflow

> **Goal:** To simulate a professional ETL pipeline for stock analytics, showcasing key concepts of data engineering: automation, scalability, reliability, and traceability.

---

## Folder Structure
stock_pipeline_project/
│
├── data/ # All raw, processed, backup files
│ ├── raw/
│ ├── processed/
│ ├── backup/
│ └── raw_today/
|
├── Notebooks/
│ ├── raw_data_summary.csv # File check
│ └── stock_pipeline_colab.ipynb
|
├── logs/ # All logs everyday
│ ├── pipeline_20250716.log # Example
│ └── pipeline_20250717.log # Example
|
├── src/ # All pipeline source code
│ ├── ingestion.py # Ingest data from Yahoo Finance
│ ├── inspection.py # Data Exploration write in notebook
│ ├── processing.py # Clean and enrich data
│ ├── schema_validation.py # data integrity check
│ ├── init_mysql_db.py # Create Database and table(If Not)
│ ├── upload_to_mysql.py # Upload processed data to MySQL
│ ├── backup.py # Backup processed data
│ └── pipeline.py # Main pipeline runner
│
│
├── config.yaml # Centralized configs
├── Data_Flow.drawio # File Data flow on Draw.io
├── Data_Flow.drawio.png # PNG file data flow
├── Stock_ID.xlsx # Stock ticker list
├── requirements.txt # Configuration
└── README.md

---

## Data Pipeline Flow




## Features
### Data Ingestion

Historical & daily data via yfinance

Auto-handling missing tickers

### Processing

Feature engineering: Moving Averages, Bollinger Bands, %Change

Duplicate handling & data quality checks

### Data Storage

MySQL as the data warehouse simulation

Backup with timestamped folders

### Logging & Error Handling

loguru for clear, colorful logs

Logs success, skipped, failed, empty files

### Config-Driven & Modular

Easily adaptable for new tickers, databases, or destinations

## Example Outputs
| ID | Ticker | Date       | Open  | High  | Low   | Close | Adj_Close | Volume  | MA10 | MA50 | Pct_Change | BB_Upper | BB_Lower |
| -- | ------ | ---------- | ----- | ----- | ----- | ----- | ---------- | ------- | ---- | ---- | ----------- | --------- | --------- |
| 1  | AAA.VN | 2024-07-16 | 12500 | 12600 | 12150 | 12200 | 11715.2    | 6840800 | NULL | NULL | NULL        | NULL      | NULL      |
| 2  | AAA.VN | 2024-07-17 | 12350 | 12350 | 11350 | 11700 | 11235.1    | 9067900 | NULL | NULL | -4.09836    | NULL      | NULL      |
| 3  | AAA.VN | 2024-07-18 | 11700 | 12050 | 11650 | 12050 | 11571.2    | 6755000 | NULL | NULL | 2.99145     | NULL      | NULL      |
| 4  | AAA.VN | 2024-07-19 | 12050 | 12050 | 11650 | 11700 | 11235.1    | 4318100 | NULL | NULL | -2.90456    | NULL      | NULL      |
| 5  | AAA.VN | 2024-07-22 | 11750 | 11800 | 11250 | 11700 | 11235.1    | 6214100 | NULL | NULL | 0.00000     | NULL      | NULL      |


## Skills Demonstrated
ETL Development in Python

Data Pipeline Design

SQL & MySQL integration

Feature Engineering for Time Series

Config & Logging Best Practices

Reproducibility & Modularity

## Author
AVT Bom21
Intern Data Engineer, passionate about real-world pipeline architecture and data infrastructure.

Contact: dannguyenle281@gmail.com
LinkedIn: www.linkedin.com/in/dân-nguyễn-383501341
GitHub: https://github.com/AvtBom21