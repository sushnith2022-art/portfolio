"""
Real-time Data Streaming Pipeline - Airflow DAG
===============================================
Apache Airflow DAG for orchestrating real-time data pipelines
with Kafka, Spark Streaming, and monitoring.

Author: Sushnith Vaidya
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

# Default arguments for all tasks
default_args = {
    'owner': 'sushnith.vaidya',
    'depends_on_past': False,
    'email': ['sushnith2022@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'realtime_data_streaming_pipeline',
    default_args=default_args,
    description='Real-time data streaming pipeline with Kafka and Spark',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'kafka', 'spark', 'realtime'],
    max_active_runs=1,
)


# ============================================
# TASK 1: Data Quality Check (Pre-processing)
# ============================================

def check_source_health(**context):
    """
    Check if source databases are healthy before starting pipeline.
    """
    import psycopg2
    
    sources = [
        {'host': 'postgres-primary', 'db': 'clinical_db'},
        {'host': 'mysql-replica', 'db': 'operations_db'}
    ]
    
    for source in sources:
        try:
            conn = psycopg2.connect(
                host=source['host'],
                database=source['db'],
                user='airflow',
                password='***'
            )
            conn.close()
            print(f"✅ {source['host']} is healthy")
        except Exception as e:
            raise Exception(f"❌ {source['host']} is down: {str(e)}")
    
    return "All sources are healthy"

source_health_check = PythonOperator(
    task_id='check_source_health',
    python_callable=check_source_health,
    dag=dag,
)


# ============================================
# TASK 2: Start Kafka Connect (CDC)
# ============================================

start_kafka_connect = BashOperator(
    task_id='start_kafka_connect',
    bash_command='''
        curl -X POST http://kafka-connect:8083/connectors \
        -H "Content-Type: application/json" \
        -d @/opt/airflow/dags/config/cdc_connector.json
    ''',
    dag=dag,
)


# ============================================
# TASK 3: Spark Streaming Job
# ============================================

spark_streaming_job = SparkSubmitOperator(
    task_id='spark_streaming_processing',
    application='/opt/spark/jobs/streaming_processor.py',
    name='RealtimeStreamingProcessor',
    conn_id='spark_default',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.streaming.checkpointLocation': 'gs://checkpoints/streaming/',
        'spark.kafka.bootstrap.servers': 'kafka:9092',
    },
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0',
    dag=dag,
)


# ============================================
# TASK 4: Data Validation with Great Expectations
# ============================================

def validate_streaming_data(**context):
    """
    Validate processed data using Great Expectations.
    """
    import great_expectations as gx
    
    context = gx.get_context()
    checkpoint = context.get_checkpoint("streaming_data_checkpoint")
    checkpoint_result = checkpoint.run()
    
    if not checkpoint_result.success:
        raise Exception("Data validation failed!")
    
    return "Data validation passed"

data_validation = PythonOperator(
    task_id='validate_streaming_data',
    python_callable=validate_streaming_data,
    dag=dag,
)


# ============================================
# TASK 5: Load to Analytics Database
# ============================================

def load_to_druid(**context):
    """
    Load processed data to Apache Druid for real-time analytics.
    """
    import requests
    
    ingestion_spec = {
        "type": "kafka",
        "spec": {
            "dataSchema": {
                "dataSource": "clinical_events",
                "timestampSpec": {"column": "event_time", "format": "iso"},
                "dimensionsSpec": {
                    "dimensions": ["patient_id", "event_type", "provider_id"]
                }
            },
            "ioConfig": {
                "topic": "processed.clinical.events",
                "consumerProperties": {"bootstrap.servers": "kafka:9092"}
            }
        }
    }
    
    response = requests.post(
        'http://druid-coordinator:8081/druid/indexer/v1/supervisor',
        json=ingestion_spec
    )
    
    if response.status_code == 200:
        return "Druid ingestion started"
    else:
        raise Exception(f"Druid ingestion failed: {response.text}")

load_to_analytics = PythonOperator(
    task_id='load_to_druid',
    python_callable=load_to_druid,
    dag=dag,
)


# ============================================
# TASK 6: Monitoring and Alerting
# ============================================

def send_success_notification(**context):
    """
    Send Slack notification on successful completion.
    """
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    slack_success_notification = SlackWebhookOperator(
        task_id='notify_slack_success',
        http_conn_id='slack_conn',  # Connection ID for Slack webhook
        message="""
        ✅ *Real-time Pipeline Completed Successfully*
        
        • DAG: {{ dag.dag_id }}
        • Run Date: {{ ds }}
        • Duration: {{ (ti.end_date - ti.start_date).total_seconds() // 60 }} minutes
        • Records Processed: {{ ti.xcom_pull(task_ids='spark_streaming_processing') }}
        
        All validations passed!
        """
    )
    
    return slack_success_notification.execute(context=context)

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    trigger_rule='all_success',  # Only run if all upstream tasks succeed
    dag=dag,
)


# ============================================
# TASK DEPENDENCIES (Pipeline Flow)
# ============================================

source_health_check >> start_kafka_connect >> spark_streaming_job
spark_streaming_job >> data_validation >> load_to_analytics
load_to_analytics >> success_notification


# ============================================
# OPTIONAL: Task Group for Complex Workflows
# ============================================

with TaskGroup('data_quality_checks', dag=dag) as quality_checks:
    
    completeness_check = BashOperator(
        task_id='check_completeness',
        bash_command='python /opt/scripts/check_completeness.py',
    )
    
    accuracy_check = BashOperator(
        task_id='check_accuracy',
        bash_command='python /opt/scripts/check_accuracy.py',
    )
    
    timeliness_check = BashOperator(
        task_id='check_timeliness',
        bash_command='python /opt/scripts/check_timeliness.py',
    )
    
    [completeness_check, accuracy_check, timeliness_check]

# Add quality checks to main flow
# spark_streaming_job >> quality_checks >> data_validation
