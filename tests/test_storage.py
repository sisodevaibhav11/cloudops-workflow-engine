def test_ensure_resources_exist(storage):
    """Verifies that the S3 bucket and DynamoDB table are created properly."""
    # S3 bucket should exist
    response = storage.s3_client.head_bucket(Bucket=storage.s3_bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # DynamoDB table should exist and be active
    table = storage.get_table()
    assert table.table_status == "ACTIVE"
    assert table.table_name == "test-OpsJobExecutionTable"


def test_save_and_get_job_execution(storage):
    """Verifies creating and retrieving a job record in DynamoDB."""
    job_id = "JOB-TEST-1001"
    item = storage.save_job_execution(
        job_id=job_id,
        state="PENDING",
        metadata={"trigger": "manual_test", "batch_size": 25},
    )

    assert item["job_id"] == job_id
    assert item["status"] == "PENDING"
    assert item["retry_count"] == 0

    fetched = storage.get_job_execution(job_id)
    assert fetched is not None
    assert fetched["job_id"] == job_id
    assert fetched["status"] == "PENDING"
    assert fetched["metadata"]["batch_size"] == 25


def test_update_job_status(storage):
    """Verifies atomic state transitions and error logging in DynamoDB."""
    job_id = "JOB-TEST-1002"
    storage.save_job_execution(job_id, "PENDING")

    updated = storage.update_job_status(
        job_id=job_id,
        new_state="FAILED",
        error_message="Simulated downstream timeout",
        s3_report_url="s3://test-amazon-ops-bucket/reports/JOB-TEST-1002/reconciliation_report.json",
        extra_metrics={"records_processed": 10, "discrepancies": 1},
    )

    assert updated["status"] == "FAILED"
    assert updated["error_message"] == "Simulated downstream timeout"
    assert updated["metrics"]["records_processed"] == 10

    # Verify persistent state in DynamoDB
    record = storage.get_job_execution(job_id)
    assert record["status"] == "FAILED"
    assert record["error_message"] == "Simulated downstream timeout"


def test_increment_retry_count(storage):
    """Verifies DynamoDB atomic increment for retry counter."""
    job_id = "JOB-TEST-1003"
    storage.save_job_execution(job_id, "PENDING")

    retry_1 = storage.increment_retry_count(job_id)
    assert retry_1 == 1

    retry_2 = storage.increment_retry_count(job_id)
    assert retry_2 == 2


def test_upload_and_download_audit_report(storage):
    """Verifies uploading JSON audit reports to S3 and downloading them back."""
    job_id = "JOB-TEST-1004"
    report_payload = {
        "job_id": job_id,
        "total_records": 100,
        "mismatches": [{"txn_id": "TXN-99", "diff": 50.0}],
        "status": "COMPLETED",
    }

    s3_uri = storage.upload_audit_report(job_id, report_payload)
    assert s3_uri.startswith("s3://test-amazon-ops-bucket/reports/JOB-TEST-1004/")

    downloaded = storage.download_audit_report(s3_uri)
    assert downloaded["job_id"] == job_id
    assert downloaded["total_records"] == 100
    assert len(downloaded["mismatches"]) == 1
    assert downloaded["mismatches"][0]["txn_id"] == "TXN-99"
