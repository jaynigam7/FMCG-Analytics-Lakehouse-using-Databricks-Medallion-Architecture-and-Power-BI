# FMCG-Analytics-Lakehouse-using-Databricks-Medallion-Architecture-and-Power-BI

## Project Overview

A retail FMCG company acquired a smaller brand and needed to consolidate customer, product, pricing and order data into a centralized Lakehouse.

## Architecture

OLTP → Bronze → Silver → Gold → Power BI

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- AWS S3
- SQL
- Power BI

## Key Features

- Medallion Architecture
- Full Load Pipeline
- Incremental Load Pipeline
- Delta MERGE
- Data Quality Checks
- Star Schema Modeling
  
fmcg-lakehouse-project/
│
├── README.md
├── architecture/
│   └── architecture.png
│
├── setup/
│   ├── setup_catalog.py
│   ├── utilities.py
│   └── dim_date_table_creation.py
│
├── bronze_silver_gold/
│   ├── customers.py
│   ├── products.py
│   └── pricing.py
│
├── fact_pipeline/
│   ├── full_load_fact.py
│   └── incremental_load_fact.py
│
├── sample_data/
│   ├── customers.csv
│   ├── products.csv
│   ├── gross_price.csv
│   └── orders.csv
│
└── powerbi/
    ├── dashboard.png
    └── model.png
    
fmcg_lakehouse_architecture
<img width="2048" height="1044" alt="Presentation1" src="https://github.com/user-attachments/assets/c8f949d3-d960-4cf9-84fc-77d6bcd43881" />

Data Flow
OLTP Sources
      │
      ▼
AWS S3 Landing Zone
      │
      ▼
Bronze Layer
(Raw Data)
      │
      ▼
Silver Layer
(Cleansed & Standardized Data)
      │
      ▼
Gold Layer
(Dimensions & Facts)
      │
      ▼
Power BI
