"""
Healthcare ETL Pipeline - PySpark Sample
========================================
This module demonstrates ETL pipeline patterns for healthcare data processing
with HIPAA compliance using PySpark.

Author: Sushnith Vaidya
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sha2, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import hashlib


# ── Spark Session ────────────────────────────────────────────────────────────
def create_spark_session(app_name="HealthcareETL"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


# ── Schema ───────────────────────────────────────────────────────────────────
PATIENT_SCHEMA = StructType([
    StructField("patient_id",    StringType(),  False),
    StructField("patient_name",  StringType(),  True),
    StructField("ssn",           StringType(),  True),
    StructField("date_of_birth", DateType(),    True),
    StructField("icd10_code",    StringType(),  True),
    StructField("age",           IntegerType(), True),
    StructField("diagnosis",     StringType(),  True),
    StructField("facility_id",   StringType(),  True),
])

# Valid ICD-10 code prefixes (sample reference set)
VALID_ICD10_PREFIXES = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
                         "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
                         "U", "V", "W", "X", "Y", "Z"}


# ── PHI Masking ──────────────────────────────────────────────────────────────
def mask_phi_column(df, column_name):
    """
    Apply SHA-256 hashing to a PHI column for HIPAA Safe Harbor compliance.
    One-way hash ensures data is irreversibly de-identified.
    """
    return df.withColumn(column_name, sha2(col(column_name).cast("string"), 256))


def apply_phi_masking(df):
    """
    Mask all 18 HIPAA Safe Harbor PHI identifiers present in dataset.
    Applies SHA-256 to: patient_name, ssn, date_of_birth.
    """
    phi_columns = ["patient_name", "ssn", "date_of_birth"]
    for phi_col in phi_columns:
        if phi_col in df.columns:
            df = mask_phi_column(df, phi_col)
    return df


# ── ICD-10 Validation ────────────────────────────────────────────────────────
def validate_icd10(df):
    """
    Validate ICD-10 codes against reference prefix set.
    Splits records into valid and invalid for quarantine routing.
    """
    df = df.withColumn(
        "icd10_valid",
        when(
            col("icd10_code").isNull(), False
        ).when(
            col("icd10_code").substr(1, 1).isin(list(VALID_ICD10_PREFIXES)), True
        ).otherwise(False)
    )
    valid_df   = df.filter(col("icd10_valid") == True).drop("icd10_valid")
    invalid_df = df.filter(col("icd10_valid") == False).drop("icd10_valid")
    return valid_df, invalid_df


# ── Schema Validation ────────────────────────────────────────────────────────
def validate_schema(df, required_columns):
    """
    Check that all required columns are present in the DataFrame.
    Raises ValueError on schema drift.
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Schema drift detected. Missing columns: {missing}")
    return df


# ── Data Quality Checks ──────────────────────────────────────────────────────
def run_quality_checks(df):
    """
    Run basic data quality assertions:
    - Row count > 0
    - patient_id has no nulls
    - icd10_code has no nulls
    """
    total_rows = df.count()
    assert total_rows > 0, "Quality check failed: DataFrame is empty."

    null_patient_ids = df.filter(col("patient_id").isNull()).count()
    assert null_patient_ids == 0, f"Quality check failed: {null_patient_ids} null patient_ids."

    null_icd10 = df.filter(col("icd10_code").isNull()).count()
    null_rate = null_icd10 / total_rows
    assert null_rate < 0.05, f"Quality check failed: ICD-10 null rate {null_rate:.2%} exceeds 5% threshold."

    print(f"✅ Quality checks passed: {total_rows} rows, 0 null patient_ids, ICD-10 null rate {null_rate:.2%}")
    return df


# ── BigQuery Load ────────────────────────────────────────────────────────────
def load_to_bigquery(df, project_id, dataset, table, partition_field="processed_date"):
    """
    Load DataFrame to a partitioned BigQuery table.
    Uses date partitioning for cost-efficient querying at scale.
    """
    df = df.withColumn("processed_date", current_timestamp().cast(DateType()))
    bq_table = f"{project_id}.{dataset}.{table}"

    (
        df.write
        .format("bigquery")
        .option("table", bq_table)
        .option("partitionField", partition_field)
        .option("partitionType", "DAY")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_APPEND")
        .save()
    )
    print(f"✅ Loaded {df.count()} rows to {bq_table}")


# ── Quarantine Write ─────────────────────────────────────────────────────────
def write_quarantine(df, gcs_bucket, run_date):
    """
    Write invalid records to GCS quarantine path for manual review.
    Invalid records are never loaded to BigQuery.
    """
    quarantine_path = f"gs://{gcs_bucket}/quarantine/{run_date}/"
    df.write.mode("overwrite").parquet(quarantine_path)
    print(f"⚠️  {df.count()} invalid records written to quarantine: {quarantine_path}")


# ── Main Pipeline ────────────────────────────────────────────────────────────
def run_pipeline(source_path, project_id, dataset, table, gcs_bucket, run_date):
    """
    End-to-end healthcare ETL pipeline:
    1. Ingest raw data from GCS
    2. Validate schema
    3. Apply PHI masking (HIPAA Safe Harbor)
    4. Validate ICD-10 codes
    5. Run data quality checks
    6. Load valid records to BigQuery
    7. Quarantine invalid records to GCS
    """
    spark = create_spark_session()

    # 1. Ingest
    print(f"📥 Ingesting data from: {source_path}")
    df = spark.read.schema(PATIENT_SCHEMA).parquet(source_path)

    # 2. Schema validation
    required_cols = ["patient_id", "icd10_code", "patient_name", "ssn"]
    df = validate_schema(df, required_cols)

    # 3. PHI masking — MUST happen before any other transformation
    print("🔒 Applying PHI masking...")
    df = apply_phi_masking(df)

    # 4. ICD-10 validation — split valid/invalid
    print("🏥 Validating ICD-10 codes...")
    valid_df, invalid_df = validate_icd10(df)

    # 5. Data quality checks on valid records
    print("🔍 Running data quality checks...")
    valid_df = run_quality_checks(valid_df)

    # 6. Load valid records to BigQuery
    print("📤 Loading to BigQuery...")
    load_to_bigquery(valid_df, project_id, dataset, table)

    # 7. Quarantine invalid records
    if invalid_df.count() > 0:
        write_quarantine(invalid_df, gcs_bucket, run_date)

    spark.stop()
    print("✅ Pipeline completed successfully.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Healthcare ETL Pipeline")
    parser.add_argument("--source",    required=True)
    parser.add_argument("--project",   required=True)
    parser.add_argument("--dataset",   required=True)
    parser.add_argument("--table",     required=True)
    parser.add_argument("--bucket",    required=True)
    parser.add_argument("--run_date",  required=True)
    args = parser.parse_args()

    run_pipeline(
        source_path=args.source,
        project_id=args.project,
        dataset=args.dataset,
        table=args.table,
        gcs_bucket=args.bucket,
        run_date=args.run_date,
    )
