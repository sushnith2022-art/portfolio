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

# ── Config ────────────────────────────────────────────────────────────────────

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

# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_data_quality(**context):
    """Query BigQuery to assert row count and null thresholds post-load."""
    hook  = BigQueryHook(use_legacy_sql=False)
    query = f"""
        SELECT
            COUNT(*)                                    AS total_rows,
            COUNTIF(patient_id IS NULL)                 AS null_ids,
            COUNTIF(admission_date > CURRENT_DATE())    AS future_dates
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE DATE(load_timestamp) = CURRENT_DATE()
    """
    rows = hook.get_records(query)
    total_rows, null_ids, future_dates = rows[0]

    assert total_rows   > 0,   f"Quality check failed: no rows loaded today."
    assert null_ids     == 0,  f"Quality check failed: {null_ids} null patient IDs."
    assert future_dates == 0,  f"Quality check failed: {future_dates} future admission dates."

    print(f"✅ Quality passed — {total_rows:,} rows, 0 nulls, 0 future dates.")

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "healthcare_pyspark_etl",
    description     = "Healthcare ETL: GCS → Dataproc PySpark → BigQuery",
    default_args    = DEFAULT_ARGS,
    start_date      = datetime(2024, 1, 1),
    schedule        = "0 3 * * *",   # daily at 03:00 UTC
    catchup         = False,
    tags            = ["healthcare", "pyspark", "dataproc", "etl"],
) as dag:

    # 1 · Confirm source file exists in GCS before doing any work
    check_source_data = GCSObjectExistenceSensor(
        task_id    = "check_source_data",
        bucket     = GCS_BUCKET,
        object     = SOURCE_OBJECT,
        timeout    = 300,
        poke_interval = 30,
    )

    # 2 · Submit PySpark job to an existing Dataproc cluster
    submit_pyspark_job_to_dataproc = DataprocSubmitPySparkJobOperator(
        task_id     = "submit_pyspark_job_to_dataproc",
        main        = PYSPARK_SCRIPT,
        arguments   = [
            f"--project={PROJECT_ID}",
            f"--source=gs://{GCS_BUCKET}/{SOURCE_OBJECT}",
            f"--dest={PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
        ],
        cluster_name = CLUSTER_NAME,
        region       = REGION,
        project_id   = PROJECT_ID,
        dataproc_jinja_translate = True,
    )

    # 3 · Assert data quality in BigQuery after the load
    validate_data_quality = PythonOperator(
        task_id         = "validate_data_quality",
        python_callable = validate_data_quality,
    )

    # 4 · Alert Slack on any upstream failure (triggered only on failure)
    notify_on_failure = SlackWebhookOperator(
        task_id         = "notify_on_failure",
        slack_webhook_conn_id = SLACK_CONN_ID,
        message         = (
            ":rotating_light: *Healthcare ETL failed*\n"
            "DAG: `{{ dag.dag_id }}`  |  Run: `{{ ds }}`\n"
            "Check the Airflow UI for details."
        ),
        trigger_rule    = "one_failed",   # runs only when a prior task fails
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    check_source_data >> submit_pyspark_job_to_dataproc >> validate_data_quality
    [check_source_data, submit_pyspark_job_to_dataproc, validate_data_quality] >> notify_on_failure
