# Cloud Migration Toolkit

## Overview
Automated migration framework for moving data warehouses from Teradata to Google BigQuery, 
including schema conversion, data validation, and performance optimization.

## Architecture
- **Source:** Teradata Data Warehouse
- **Target:** Google BigQuery
- **ETL:** Python-based migration pipelines
- **Transformation:** dbt (data build tool)
- **Validation:** Automated data quality checks

```mermaid
flowchart TD
    A[🗄️ Teradata Data Warehouse] -->|Extract Data| B[🐍 Python Pipeline]
    B -->|Orchestrated by| C[⚡ Apache Airflow]
    B -->|Transform & Model| D[🔧 dbt]
    D -->|Load Partitioned Data| E[☁️ Google BigQuery]
    E -->|Validate| F[✅ Data Quality Checks]
    F -->|Pass| G[✨ Production Ready]
    F -->|Fail| H[🚨 Alert & Rollback]

    style A fill:#f4a261,stroke:#e76f51,color:#000
    style B fill:#457b9d,stroke:#1d3557,color:#fff
    style C fill:#e63946,stroke:#c1121f,color:#fff
    style D fill:#2a9d8f,stroke:#264653,color:#fff
    style E fill:#4895ef,stroke:#4361ee,color:#fff
    style F fill:#7209b7,stroke:#560bad,color:#fff
    style G fill:#06d6a0,stroke:#059669,color:#000
    style H fill:#ef233c,stroke:#d90429,color:#fff
```

## Technologies Used
- Python, SQL
- Google BigQuery
- Teradata
- dbt (data build tool)
- Apache Airflow

## Key Features
✅ Automated schema conversions from Teradata to BigQuery  
✅ Incremental data migration with CDC  
✅ Data validation and reconciliation  
✅ Query performance optimization  
✅ Rollback capabilities  

## Migration Process
1. **Schema Analysis** - Extract and convert Teradata schemas
2. **Data Extraction** - Export data in optimized batches
3. **Transformation** - Convert SQL dialects and optimize
4. **Loading** - Load to BigQuery with partitioning
5. **Validation** - Reconcile row counts and sample data

## Project Status
🚧 This project demonstrates real-world migration patterns 
applied during Teradata to BigQuery migrations at enterprise scale.

---
*Part of [Sushnith Vaidya's Data Engineering Portfolio](https://github.com/sushnith2022-art/portfolio)*
