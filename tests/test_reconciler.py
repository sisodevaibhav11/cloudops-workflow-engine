import json
from unittest.mock import patch
import pytest
from src.reconciler import ReconciliationEngine
from src.state_machine import JobState


def test_analyze_records_discrepancy_detection(storage):
    """Verifies discrepancy detection logic, amounts, and variance calculations."""
    engine = ReconciliationEngine(storage_mgr=storage, discrepancy_threshold_pct=15.0)

    records = [
        {"transaction_id": "T1", "order_id": "O1", "amount": 100.0, "expected_amount": 100.0, "status": "CAPTURED"},
        {"transaction_id": "T2", "order_id": "O2", "amount": 150.0, "expected_amount": 100.0, "status": "CAPTURED"},  # Overcharged by 50
        {"transaction_id": "T3", "order_id": "O3", "amount": 0.0, "expected_amount": 80.0, "status": "UNPAID_ZERO_VALUED"}, # Zero valued
    ]

    analysis = engine.analyze_records(records)
    summary = analysis["summary"]

    assert summary["total_records"] == 3
    assert summary["clean_records"] == 1
    assert summary["discrepancies_detected"] == 2
    assert summary["net_variance"] == -30.0  # (250.0 - 280.0)
    assert summary["ops_alert_triggered"] is True  # 66.67% > 15% threshold

    discrepancies = analysis["discrepancies"]
    assert len(discrepancies) == 2
    assert discrepancies[0]["issue_type"] == "OVERCHARGED"
    assert discrepancies[1]["issue_type"] == "ZERO_VALUED"


def test_execute_job_end_to_end(storage):
    """Verifies end-to-end execution: PENDING -> RUNNING -> VALIDATING -> COMPLETED,

    saving metadata in DynamoDB and publishing the audit report to S3.
    """
    engine = ReconciliationEngine(storage_mgr=storage)

    with open("data/sample_payload.json", "r") as f:
        records = json.load(f)

    job_id = "JOB-TEST-E2E-001"
    result = engine.execute_job(records, job_id=job_id)

    assert result["job_id"] == job_id
    assert result["status"] == JobState.COMPLETED.value
    assert result["s3_report_url"].startswith("s3://test-amazon-ops-bucket/reports/JOB-TEST-E2E-001/")

    # Verify DynamoDB record
    db_item = storage.get_job_execution(job_id)
    assert db_item is not None
    assert db_item["status"] == "COMPLETED"
    assert db_item["s3_report_url"] == result["s3_report_url"]
    assert db_item["metrics"]["total_records"] == 5
    assert db_item["metrics"]["discrepancies_detected"] == 2

    # Verify S3 report content
    s3_report = storage.download_audit_report(result["s3_report_url"])
    assert s3_report["job_id"] == job_id
    assert len(s3_report["discrepancies"]) == 2
    assert s3_report["clean_records_count"] == 3


def test_execute_job_failure_handling(storage):
    """Verifies that an unhandled downstream error transitions DynamoDB to FAILED and increments retries."""
    engine = ReconciliationEngine(storage_mgr=storage)
    job_id = "JOB-TEST-FAIL-001"

    # Simulate S3 upload failure
    with patch.object(storage, "upload_audit_report", side_effect=RuntimeError("S3 Connection Interrupted")):
        with pytest.raises(RuntimeError, match="S3 Connection Interrupted"):
            engine.execute_job([{"transaction_id": "T1", "amount": 10.0, "expected_amount": 10.0}], job_id=job_id)

    # Check DynamoDB state was marked as FAILED with error message
    db_item = storage.get_job_execution(job_id)
    assert db_item is not None
    assert db_item["status"] == "FAILED"
    assert "S3 Connection Interrupted" in db_item["error_message"]
    assert db_item["retry_count"] == 1
