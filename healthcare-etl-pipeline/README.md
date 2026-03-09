# Healthcare ETL Pipeline

## Overview
A scalable ETL pipeline for processing healthcare data with HIPAA compliance, 
built using PySpark, Apache Airflow, and Google Cloud Platform.

## Architecture
```mermaid
flowchart TD
    A[🏥 Clinical Data Sources] -->|Ingest| B[☁️ GCP Cloud Storage]
    B -->|Process| C[⚡ PySpark on Dataproc]
    C -->|Mask PHI| D[🔒 HIPAA Compliance Layer]
    D -->|Validate| E[✅ Great Expectations]
    E -->|Orchestrate| F[🌀 Apache Airflow]
    F -->|Load| G[📊 BigQuery]
    E -->|Pass| G
    E -->|Fail| H[🚨 Rejected Records Table]

    style A fill:#f4a261,stroke:#e76f51,color:#000
    style B fill:#4895ef,stroke:#4361ee,color:#fff
    style C fill:#457b9d,stroke:#1d3557,color:#fff
    style D fill:#e63946,stroke:#c1121f,color:#fff
    style E fill:#2a9d8f,stroke:#264653,color:#fff
    style F fill:#7209b7,stroke:#560bad,color:#fff
    style G fill:#06d6a0,stroke:#059669,color:#000
    style H fill:#ef233c,stroke:#d90429,color:#fff
```

## Architecture Details
- **Processing:** PySpark on GCP Dataproc
- **Orchestration:** Apache Airflow
- **Storage:** BigQuery for data warehouse
- **Compliance:** SHA-256 data masking for HIPAA
- **Quality:** Great Expectations validation

## Technologies Used
- Python, PySpark
- Apache Airflow
- Google Cloud Platform (BigQuery, Dataproc, Cloud Storage)
- Great Expectations (data quality)

## Key Features
✅ HIPAA-compliant PHI masking with SHA-256 encryption  
✅ Schema validation with PySpark StructType  
✅ Automated data quality checks  
✅ Valid/Invalid record separation  
✅ Partitioned output to BigQuery  
✅ Orchestrated workflows with Airflow  

## Project Status
✅ This project demonstrates real-world healthcare ETL patterns
with HIPAA compliance applied at CVS Health.

---
*Part of [Sushnith Vaidya's Data Engineering Portfolio](https://github.com/sushnith2022-art/portfolio)*
