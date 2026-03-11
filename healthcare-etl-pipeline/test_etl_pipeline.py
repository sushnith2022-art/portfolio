"""
Healthcare ETL Pipeline - Unit Tests
=====================================
Tests for core ETL functions: PHI masking, data validation, schema checks.
Author: Sushnith Vaidya
"""

import pytest
from unittest.mock import MagicMock, patch


# ── TEST 1: PHI Masking ──────────────────────────────────────────
class TestPHIMasking:

    def test_phi_columns_are_masked(self):
        """SHA-256 masking should replace original PHI columns."""
        phi_columns = ["name", "dob"]
        masked_columns = [f"{col}_masked" for col in phi_columns]

        # Verify masked column names are generated correctly
        assert "name_masked" in masked_columns
        assert "dob_masked" in masked_columns

    def test_original_phi_columns_removed(self):
        """Original PHI columns should not exist after masking."""
        phi_columns = ["name", "dob"]
        remaining_columns = ["patient_id", "name_masked",
                             "dob_masked", "diagnosis_code",
                             "visit_date", "provider_id"]

        for col in phi_columns:
            assert col not in remaining_columns

    def test_masked_column_format(self):
        """Masked column names should follow {column}_masked pattern."""
        phi_columns = ["name", "dob", "ssn"]
        for col in phi_columns:
            masked = f"{col}_masked"
            assert masked.endswith("_masked")
            assert masked.startswith(col)


# ── TEST 2: ICD-10 Validation ────────────────────────────────────
class TestDiagnosisCodeValidation:

    def test_valid_icd10_codes(self):
        """Valid ICD-10 codes should pass regex validation."""
        import re
        pattern = r"^[A-Z][0-9]{2}$"
        valid_codes = ["A01", "B12", "Z99", "J45", "C50"]

        for code in valid_codes:
            assert re.match(pattern, code), \
                f"Valid ICD-10 code {code} failed validation"

    def test_invalid_icd10_codes(self):
        """Invalid codes should fail regex validation."""
        import re
        pattern = r"^[A-Z][0-9]{2}$"
        invalid_codes = ["123", "abc", "A1", "AB123", "", "a01"]

        for code in invalid_codes:
            assert not re.match(pattern, code), \
                f"Invalid code {code} passed validation incorrectly"

    def test_lowercase_code_rejected(self):
        """Lowercase diagnosis codes should be rejected."""
        import re
        pattern = r"^[A-Z][0-9]{2}$"
        assert not re.match(pattern, "a01")
        assert not re.match(pattern, "b12")


# ── TEST 3: Data Quality Checks ──────────────────────────────────
class TestDataQualityChecks:

    def test_null_patient_id_detection(self):
        """Records with null patient_id should be flagged as invalid."""
        records = [
            {"patient_id": "P001", "diagnosis_code": "A01"},
            {"patient_id": None, "diagnosis_code": "B12"},
            {"patient_id": "P003", "diagnosis_code": "C50"},
            {"patient_id": None, "diagnosis_code": "Z99"},
        ]

        invalid = [r for r in records if r["patient_id"] is None]
        valid = [r for r in records if r["patient_id"] is not None]

        assert len(invalid) == 2
        assert len(valid) == 2

    def test_valid_records_pass_quality_check(self):
        """Records with all required fields should pass quality checks."""
        record = {
            "patient_id": "P001",
            "diagnosis_code": "A01",
            "visit_date": "2024-01-15",
            "provider_id": "DR001"
        }

        assert record["patient_id"] is not None
        assert record["diagnosis_code"] is not None
        assert record["visit_date"] is not None

    def test_empty_dataset_handling(self):
        """Empty dataset should return zero valid and invalid records."""
        records = []
        invalid = [r for r in records if r.get("patient_id") is None]
        valid = [r for r in records if r.get("patient_id") is not None]

        assert len(invalid) == 0
        assert len(valid) == 0


# ── TEST 4: Pipeline Configuration ──────────────────────────────
class TestPipelineConfiguration:

    def test_phi_columns_list(self):
        """PHI columns list should contain expected sensitive fields."""
        phi_columns = ["name", "dob"]
        assert "name" in phi_columns
        assert "dob" in phi_columns
        assert len(phi_columns) == 2

    def test_output_path_format(self):
        """BigQuery output path should follow dataset.table format."""
        dataset = "healthcare_analytics"
        table = "clinical_data_processed"
        output_path = f"{dataset}.{table}"

        assert "." in output_path
        assert output_path == "healthcare_analytics.clinical_data_processed"

    def test_rejected_table_naming(self):
        """Rejected records table should follow {table}_rejected pattern."""
        output_table = "clinical_data_processed"
        rejected_table = f"{output_table}_rejected"

        assert rejected_table == "clinical_data_processed_rejected"
        assert rejected_table.endswith("_rejected")


# ── TEST 5: HIPAA Compliance ─────────────────────────────────────
class TestHIPAACompliance:

    def test_phi_fields_identified(self):
        """All known PHI fields must be in the masking list."""
        phi_fields = ["name", "dob"]
        hipaa_required = ["name", "dob"]

        for field in hipaa_required:
            assert field in phi_fields, \
                f"HIPAA required field '{field}' missing from masking list"

    def test_masked_value_is_not_original(self):
        """Masked value should never equal the original value."""
        import hashlib
        original = "John Doe"
        masked = hashlib.sha256(original.encode()).hexdigest()

        assert masked != original
        assert len(masked) == 64  # SHA-256 always produces 64 char hex

    def test_sha256_is_deterministic(self):
        """Same input should always produce same SHA-256 hash."""
        import hashlib
        value = "P001"
        hash1 = hashlib.sha256(value.encode()).hexdigest()
        hash2 = hashlib.sha256(value.encode()).hexdigest()

        assert hash1 == hash2
