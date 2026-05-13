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


class HealthcareETLPipeline:
    """
    ETL Pipeline for processing healthcare data with HIPAA compliance.
    
    Features:
    - Data ingestion from multiple sources
    - PHI/PII data masking and encryption
    - Data quality validation
    - Partitioned output to BigQuery
    """
    
    def __init__(self, app_name="HealthcareETL"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def read_clinical_data(self, input_path, file_format="parquet"):
        """
        Read clinical data from source with schema validation.
        
        Args:
            input_path: Path to input data
            file_format: File format (parquet, csv, json)
        
        Returns:
            DataFrame with clinical data
        """
        # Define schema for clinical data
        clinical_schema = StructType([
            StructField("patient_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("dob", DateType(), True),
            StructField("diagnosis_code", StringType(), True),
            StructField("medication", StringType(), True),
            StructField("visit_date", DateType(), True),
            StructField("provider_id", StringType(), True)
        ])
        
        df = self.spark.read \
            .format(file_format) \
            .schema(clinical_schema) \
            .load(input_path)
        
        print(f"Loaded {df.count()} records from {input_path}")
        return df
    
    def mask_phi_data(self, df, columns_to_mask):
        """
        Apply HIPAA-compliant masking to PHI columns.
        
        Args:
            df: Input DataFrame
            columns_to_mask: List of column names to mask
        
        Returns:
            DataFrame with masked PHI data
        """
        df_masked = df
        
        for column in columns_to_mask:
            if column in df.columns:
                # Use SHA-256 hashing for consistent anonymization
                df_masked = df_masked.withColumn(
                    f"{column}_masked",
                    sha2(col(column).cast("string"), 256)
                ).drop(column)
        
        return df_masked
    
    def validate_data_quality(self, df):
        """
        Perform data quality checks on the dataset.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        # Check for null patient IDs (critical field)
        valid_df = df.filter(col("patient_id").isNotNull())
        invalid_df = df.filter(col("patient_id").isNull())
        
        # Check for valid diagnosis codes
        valid_df = valid_df.filter(col("diagnosis_code").rlike("^[A-Z][0-9]{2}$"))
        
        # Add audit timestamp
        valid_df = valid_df.withColumn("processed_at", current_timestamp())
        
        print(f"Valid records: {valid_df.count()}")
        print(f"Invalid records: {invalid_df.count()}")
        
        return valid_df, invalid_df
    
    def write_to_bigquery(self, df, dataset, table, partition_col=None):
        """
        Write DataFrame to BigQuery with partitioning.
        
        Args:
            df: DataFrame to write
            dataset: BigQuery dataset name
            table: BigQuery table name
            partition_col: Column to partition by
        """
        output_path = f"{dataset}.{table}"
        
        writer = df.write \
            .format("bigquery") \
            .option("table", output_path) \
            .mode("append")
        
        if partition_col:
            writer = writer.partitionBy(partition_col)
        
        writer.save()
        print(f"Data written to {output_path}")
    
    def run_pipeline(self, input_path, output_dataset, output_table):
        """
        Execute the complete ETL pipeline.
        
        Args:
            input_path: Input data path
            output_dataset: Output BigQuery dataset
            output_table: Output BigQuery table
        """
        try:
            # Step 1: Extract
            raw_df = self.read_clinical_data(input_path)
            
            # Step 2: Transform - Mask PHI
            phi_columns = ["name", "dob"]
            masked_df = self.mask_phi_data(raw_df, phi_columns)
            
            # Step 3: Validate
            valid_df, invalid_df = self.validate_data_quality(masked_df)
            
            # Step 4: Load
            self.write_to_bigquery(
                valid_df, 
                output_dataset, 
                output_table,
                partition_col="visit_date"
            )
            
            # Save invalid records for review
            if invalid_df.count() > 0:
                self.write_to_bigquery(
                    invalid_df,
                    output_dataset,
                    f"{output_table}_rejected"
                )
            
            print("Pipeline completed successfully!")
            
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise


# Example usage
if __name__ == "__main__":
    pipeline = HealthcareETLPipeline()
    
    pipeline.run_pipeline(
        input_path="gs://healthcare-raw-data/clinical/",
        output_dataset="healthcare_analytics",
        output_table="clinical_data_processed"
    )
