# Real-time Data Streaming Pipeline

## Overview
A real-time data ingestion and processing pipeline using Apache Kafka and Spark Streaming, 
with Change Data Capture (CDC) for near real-time analytics.

## Architecture
```mermaid
flowchart TD
    A[🗄️ PostgreSQL & MySQL] -->|CDC Events| B[🔄 Debezium CDC]
    B -->|Stream| C[📨 Apache Kafka]
    C -->|Process| D[⚡ Spark Streaming]
    D -->|Validate| E[✅ Great Expectations]
    E -->|Pass| F[📊 Apache Druid]
    E -->|Fail| G[🚨 Alert & Dead Letter Queue]
    F -->|Serve| H[📈 Real-time Dashboard]
    I[🌀 Apache Airflow] -->|Orchestrate| D

    style A fill:#f4a261,stroke:#e76f51,color:#000
    style B fill:#e63946,stroke:#c1121f,color:#fff
    style C fill:#457b9d,stroke:#1d3557,color:#fff
    style D fill:#4895ef,stroke:#4361ee,color:#fff
    style E fill:#2a9d8f,stroke:#264653,color:#fff
    style F fill:#7209b7,stroke:#560bad,color:#fff
    style G fill:#ef233c,stroke:#d90429,color:#fff
    style H fill:#06d6a0,stroke:#059669,color:#000
    style I fill:#f4a261,stroke:#e76f51,color:#000
```

## Architecture Details
- **Data Source:** Operational databases (PostgreSQL, MySQL)
- **CDC:** Debezium for change data capture
- **Streaming:** Apache Kafka as message broker
- **Processing:** Spark Streaming for real-time transformations
- **Validation:** Great Expectations for data quality
- **Storage:** Apache Druid for real-time analytics
- **Orchestration:** Apache Airflow

## Technologies Used
- Apache Kafka
- Spark Streaming
- Python, PySpark
- Debezium (CDC)
- PostgreSQL, MySQL
- Apache Druid
- Great Expectations
- Apache Airflow

## Key Features
✅ Real-time data ingestion with sub-second latency  
✅ Change Data Capture (CDC) from multiple sources  
✅ Scalable stream processing with Spark  
✅ Fault-tolerant with exactly-once semantics  
✅ Great Expectations data validation  
✅ Real-time analytics with Apache Druid  

## Pipeline Flow
```
Database → Debezium CDC → Kafka → Spark Streaming → Great Expectations → Druid → Dashboard
```

## Project Status
✅ This project demonstrates real-world streaming architecture
with CDC implementation applied at CVS Health.

---
*Part of [Sushnith Vaidya's Data Engineering Portfolio](https://github.com/sushnith2022-art/portfolio)*
