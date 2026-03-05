# Data Quality Framework

## Overview
An automated data validation and quality monitoring system using Great Expectations,
Apache Airflow, and Monte Carlo for end-to-end data observability.

## Architecture
```mermaid
flowchart TD
    A[📥 Data Sources] -->|Ingest| B[🐍 Python Pipeline]
    B -->|Validate| C[✅ Great Expectations]
    C -->|Orchestrate| D[⚡ Apache Airflow]
    D -->|Monitor| E[🔭 Monte Carlo]
    E -->|Track Lineage| F[🔗 OpenLineage]
    C -->|Pass| G[✨ Production Data]
    C -->|Fail| H[🚨 Alert via Slack/Email]

    style A fill:#f4a261,stroke:#e76f51,color:#000
    style B fill:#457b9d,stroke:#1d3557,color:#fff
    style C fill:#2a9d8f,stroke:#264653,color:#fff
    style D fill:#e63946,stroke:#c1121f,color:#fff
    style E fill:#7209b7,stroke:#560bad,color:#fff
    style F fill:#4895ef,stroke:#4361ee,color:#fff
    style G fill:#06d6a0,stroke:#059669,color:#000
    style H fill:#ef233c,stroke:#d90429,color:#fff
```

## Architecture Details
- **Validation:** Great Expectations for data quality checks
- **Orchestration:** Apache Airflow for scheduling
- **Monitoring:** Monte Carlo for data observability
- **Lineage:** OpenLineage for data lineage tracking
- **Alerting:** Email/Slack notifications for anomalies

## Technologies Used
- Python
- Great Expectations
- Apache Airflow
- Monte Carlo
- OpenLineage
- PostgreSQL (metadata store)

## Key Features
✅ Automated data validation suites  
✅ Schema, completeness, and anomaly checks  
✅ Data lineage tracking and visualization  
✅ SLA monitoring and alerting  
✅ HIPAA compliance validation  
✅ Integration with CI/CD pipelines  

## Validation Types

| Check Type | Description |
|---|---|
| Schema | Column names, data types, nullability |
| Completeness | Null values, empty strings |
| Uniqueness | Primary key validation |
| Range | Min/max values, distributions |
| Referential | Foreign key relationships |
| Custom | Business rule validation |

## Project Status
✅ This project demonstrates real-world data governance patterns
applied to healthcare datasets with HIPAA compliance requirements at CVS Health.

---
*Part of [Sushnith Vaidya's Data Engineering Portfolio](https://github.com/sushnith2022-art/portfolio)*
