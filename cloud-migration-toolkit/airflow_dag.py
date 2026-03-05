"""Teradata → BigQuery migration DAG."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

TD_TABLE  = Variable.get("TD_SOURCE_TABLE",  default_var="prod.customers")
BQ_TABLE  = Variable.get("BQ_DEST_TABLE",    default_var="project.dataset.customers")
DBT_MODEL = Variable.get("DBT_MODEL_NAME",   default_var="customers_cleaned")

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-alerts@company.com"],
}

# ── Task callables ────────────────────────────────────────────────────────────

def extract_from_teradata(**ctx):
    """Pull data from Teradata and stage to GCS."""
    from airflow.providers.teradata.hooks.teradata import TeradataHook
    hook = TeradataHook(teradata_conn_id="teradata_default")
    rows = hook.get_records(f"SELECT * FROM {TD_TABLE}")
    if not rows:
        raise ValueError(f"No data returned from {TD_TABLE}")
    ctx["ti"].xcom_push(key="row_count", value=len(rows))
    log.info("Extracted %d rows from %s", len(rows), TD_TABLE)


def validate_schema(**ctx):
    """Assert required columns exist before transformation."""
    REQUIRED_COLUMNS = {"customer_id", "email", "created_at"}
    from airflow.providers.teradata.hooks.teradata import TeradataHook
    hook   = TeradataHook(teradata_conn_id="teradata_default")
    cols   = {r[0].lower() for r in hook.get_records(
        f"SELECT ColumnName FROM DBC.ColumnsV WHERE TableName='{TD_TABLE.split('.')[-1]}'"
    )}
    missing = REQUIRED_COLUMNS - cols
    if missing:
        raise ValueError(f"Schema validation failed – missing columns: {missing}")
    log.info("Schema validated. Columns present: %s", cols)


def transform_with_dbt(**ctx):
    """Invoke dbt run for the target model."""
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--select", DBT_MODEL, "--profiles-dir", "/opt/dbt"],
        capture_output=True, text=True, timeout=600,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run failed for model '{DBT_MODEL}'")


def validate_data_quality(**ctx):
    """Row-count reconciliation: source vs destination."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    src_count = ctx["ti"].xcom_pull(task_ids="extract_from_teradata", key="row_count")
    bq_hook   = BigQueryHook(gcp_conn_id="google_cloud_default")
    bq_count  = next(bq_hook.get_records(f"SELECT COUNT(*) FROM `{BQ_TABLE}`"))[0]
    if src_count != bq_count:
        raise ValueError(f"Row mismatch – Teradata: {src_count} | BigQuery: {bq_count}")
    log.info("Data quality passed. %d rows validated.", bq_count)

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "teradata_to_bigquery_migration",
    default_args    = DEFAULT_ARGS,
    start_date      = datetime(2024, 1, 1),
    schedule_interval = None,   # trigger manually or via CI/CD
    catchup         = False,
    max_active_runs = 1,
    tags            = ["migration", "teradata", "bigquery"],
) as dag:

    t1 = PythonOperator(task_id="extract_from_teradata", python_callable=extract_from_teradata)
    t2 = PythonOperator(task_id="validate_schema",       python_callable=validate_schema)
    t3 = PythonOperator(task_id="transform_with_dbt",    python_callable=transform_with_dbt)
    t4 = BigQueryInsertJobOperator(
            task_id      = "load_to_bigquery",
            gcp_conn_id  = "google_cloud_default",
            configuration= {"query": {"query": f"SELECT * FROM `{DBT_MODEL}_staging`",
                                       "destinationTable": {"tableId": BQ_TABLE},
                                       "writeDisposition": "WRITE_TRUNCATE",
                                       "useLegacySql": False}},
         )
    t5 = PythonOperator(task_id="validate_data_quality", python_callable=validate_data_quality)

    t1 >> t2 >> t3 >> t4 >> t5
