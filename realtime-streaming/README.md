# Real-time Data Streaming Pipeline

## Overview
A real-time data ingestion and processing pipeline using Apache Kafka and Spark Streaming, 
with Change Data Capture (CDC) for near real-time analytics.

## Architecture
- **Data Source:** Operational databases (PostgreSQL, MySQL)
- **CDC:** Debezium for change data capture
- **Streaming:** Apache Kafka as message broker
- **Processing:** Spark Streaming for real-time transformations
- **Storage:** Apache Druid for real-time analytics

## Technologies Used
- Apache Kafka
- Spark Streaming
- Python, PySpark
- Debezium (CDC)
- PostgreSQL, MySQL
- Apache Druid

## Key Features
✅ Real-time data ingestion with sub-second latency  
✅ Change Data Capture (CDC) from multiple sources  
✅ Scalable stream processing with Spark  
✅ Fault-tolerant with exactly-once semantics  
✅ Real-time analytics dashboard support  

## Pipeline Flow

Database → Debezium CDC → Kafka → Spark Streaming → Druid → Dashboard


## Project Status
🚧 This project showcases streaming architecture from my experience at CVS Health. 
Code samples are sanitized for confidentiality.

---
*Part of [Sushnith Vaidya's Data Engineering Portfolio](https://github.com/sushnith2022-art/portfolio)*
