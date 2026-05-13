"""
Healthcare PySpark ETL Pipeline — GCP Dataproc
Airflow DAG: check source → run PySpark → validate quality → notify on failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# — Config ——————————————————————————————————————————————————————
PROJECT_ID      = "my-gcp-project"
REGION          = "us-central1"
CLUSTER_NAME    = "healthcare-etl-cluster"
GCS_BUCKET      = "healthcare-data-bucket"
PYSPARK_SCRIPT  = f"gs://{GCS_BUCKET}/scripts/healthcare_etl.py"
SOURCE_OBJECT   = "raw/patients/latest/data.parquet"
BQ_DATASET      = "healthcare_dw"
BQ_TABLE        = "patients_processed"
SLACK_CONN_ID   = "slack_webhook_default"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-alerts@healthcare.org"],
}

# — DAG ——————————————————————————————————————————————————————————
with DAG(
    dag_id="healthcare_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="HIPAA-compliant healthcare ETL: GCS → Dataproc/PySpark → BigQuery",
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "etl", "hipaa", "gcp"],
) as dag:

    # ── Task 1: Wait for source file ────────────────────────────
    wait_for_source = GCSObjectExistenceSensor(
        task_id="wait_for_source_file",
        bucket=GCS_BUCKET,
        object=SOURCE_OBJECT,
        timeout=3600,
        poke_interval=120,
    )

    # ── Task 2: Submit PySpark ETL job on Dataproc ───────────────
    run_pyspark_etl = DataprocSubmitPySparkJobOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_SCRIPT,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=PROJECT_ID,
        arguments=[
            f"--source=gs://{GCS_BUCKET}/{SOURCE_OBJECT}",
            f"--project={PROJECT_ID}",
            f"--dataset={BQ_DATASET}",
            f"--table={BQ_TABLE}",
        ],
        dataproc_jars=[
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
        ],
    )

    # ── Task 3: Validate row count in BigQuery ───────────────────
    def validate_bq_row_count(**context):
        hook = BigQueryHook(use_legacy_sql=False)
        query = f"""
            SELECT COUNT(*) as row_count
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE DATE(processed_at) = CURRENT_DATE()
        """
        result = hook.get_first(query)
        row_count = result[0] if result else 0
        if row_count == 0:
            raise ValueError("Data quality check failed: 0 rows loaded today.")
        print(f"✅ Validation passed: {row_count} rows loaded.")
        return row_count

    validate_data = PythonOperator(
        task_id="validate_bq_row_count",
        python_callable=validate_bq_row_count,
    )

    # ── Task 4: Slack alert on failure ───────────────────────────
    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":red_circle: *Healthcare ETL Pipeline Failed*\n"
            f"DAG: `healthcare_etl_pipeline`\n"
            f"Date: {{{{ ds }}}}\n"
            "Please check Airflow logs immediately."
        ),
        trigger_rule="one_failed",
    )

    # ── Dependencies ─────────────────────────────────────────────
    wait_for_source >> run_pyspark_etl >> validate_data >> notify_failure
