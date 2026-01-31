-- ============================================
-- Cloud Migration Toolkit - SQL Samples
-- Teradata to BigQuery Migration Patterns
-- Author: Sushnith Vaidya
-- ============================================

-- ============================================
-- 1. SCHEMA CONVERSION EXAMPLES
-- ============================================

-- Teradata: Creating a table with PI (Primary Index)
-- CREATE TABLE claims (
--     claim_id VARCHAR(20),
--     patient_id VARCHAR(20),
--     claim_amount DECIMAL(18,2),
--     claim_date DATE
-- ) PRIMARY INDEX (claim_id);

-- BigQuery Equivalent with Partitioning
CREATE TABLE IF NOT EXISTS `healthcare_analytics.claims` (
    claim_id STRING NOT NULL,
    patient_id STRING,
    claim_amount NUMERIC,
    claim_date DATE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY claim_date
CLUSTER BY patient_id;

-- ============================================
-- 2. DATA TYPE MAPPING
-- ============================================

-- Teradata to BigQuery Data Type Conversion
/*
Teradata          -> BigQuery
VARCHAR(n)        -> STRING
CHAR(n)           -> STRING
INTEGER           -> INT64
DECIMAL(p,s)      -> NUMERIC
DATE              -> DATE
TIMESTAMP(n)      -> TIMESTAMP
BYTEINT           -> INT64
SMALLINT          -> INT64
BIGINT            -> INT64
FLOAT             -> FLOAT64
*/

-- ============================================
-- 3. ETL MIGRATION QUERY
-- ============================================

-- Incremental Load with CDC (Change Data Capture)
MERGE `healthcare_analytics.claims` T
USING (
    SELECT 
        claim_id,
        patient_id,
        claim_amount,
        claim_date,
        ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) as rn
    FROM `staging.claims_incremental`
    WHERE updated_at > (SELECT MAX(processed_at) FROM `healthcare_analytics.claims`)
) S
ON T.claim_id = S.claim_id AND S.rn = 1
WHEN MATCHED THEN
    UPDATE SET 
        T.patient_id = S.patient_id,
        T.claim_amount = S.claim_amount,
        T.claim_date = S.claim_date,
        T.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (claim_id, patient_id, claim_amount, claim_date, processed_at)
    VALUES (S.claim_id, S.patient_id, S.claim_amount, S.claim_date, CURRENT_TIMESTAMP());

-- ============================================
-- 4. DATA VALIDATION QUERIES
-- ============================================

-- Row Count Validation
SELECT 
    'Source (Teradata)' as source,
    COUNT(*) as row_count
FROM `staging.claims_teradata`

UNION ALL

SELECT 
    'Target (BigQuery)' as source,
    COUNT(*) as row_count
FROM `healthcare_analytics.claims`;

-- Data Reconciliation - Check for Mismatches
SELECT 
    t.claim_id,
    t.claim_amount as teradata_amount,
    b.claim_amount as bigquery_amount,
    ABS(t.claim_amount - b.claim_amount) as difference
FROM `staging.claims_teradata` t
FULL OUTER JOIN `healthcare_analytics.claims` b
    ON t.claim_id = b.claim_id
WHERE 
    t.claim_amount != b.claim_amount
    OR t.claim_id IS NULL 
    OR b.claim_id IS NULL;

-- ============================================
-- 5. PERFORMANCE OPTIMIZATION
-- ============================================

-- Query with Partition Pruning (Efficient)
SELECT 
    claim_id,
    patient_id,
    claim_amount
FROM `healthcare_analytics.claims`
WHERE claim_date BETWEEN '2024-01-01' AND '2024-01-31';

-- Materialized View for Common Aggregations
CREATE MATERIALIZED VIEW `healthcare_analytics.mv_monthly_claims`
AS
SELECT 
    DATE_TRUNC(claim_date, MONTH) as claim_month,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount
FROM `healthcare_analytics.claims`
GROUP BY 1;

-- ============================================
-- 6. DATA QUALITY CHECKS
-- ============================================

-- Null Value Check
SELECT 
    'patient_id' as column_name,
    COUNT(*) - COUNT(patient_id) as null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(patient_id)) / COUNT(*), 2) as null_percentage
FROM `healthcare_analytics.claims`

UNION ALL

SELECT 
    'claim_amount' as column_name,
    COUNT(*) - COUNT(claim_amount) as null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(claim_amount)) / COUNT(*), 2) as null_percentage
FROM `healthcare_analytics.claims`;

-- Duplicate Check
SELECT 
    claim_id,
    COUNT(*) as occurrence_count
FROM `healthcare_analytics.claims`
GROUP BY claim_id
HAVING COUNT(*) > 1;

-- ============================================
-- 7. DIMENSIONAL MODELING - STAR SCHEMA
-- ============================================

-- Date Dimension Table
CREATE TABLE IF NOT EXISTS `healthcare_analytics.dim_date` (
    date_key INT64 NOT NULL,
    full_date DATE,
    day_of_week INT64,
    day_name STRING,
    month INT64,
    month_name STRING,
    quarter INT64,
    year INT64,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Patient Dimension Table
CREATE TABLE IF NOT EXISTS `healthcare_analytics.dim_patient` (
    patient_key INT64 NOT NULL,
    patient_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    insurance_type STRING,
    effective_date DATE,
    expiration_date DATE
);

-- Claims Fact Table (with surrogate keys)
CREATE TABLE IF NOT EXISTS `healthcare_analytics.fact_claims` (
    claim_key INT64 NOT NULL,
    claim_id STRING,
    patient_key INT64,
    date_key INT64,
    provider_key INT64,
    claim_amount NUMERIC,
    paid_amount NUMERIC,
    claim_status STRING
);
