import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from src.api import app, storage_manager, reconciliation_engine


@pytest.fixture
def client():
    """Provides a TestClient wrapped with mock_aws context."""
    with mock_aws():
        # Re-initialize storage and resources inside the mock_aws context
        storage_manager.ensure_resources_exist()
        yield TestClient(app)


def test_api_health_check(client):
    """Tests the /health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert "OpsPulse" in data["service"]


def test_api_trigger_and_query_workflow(client):
    """Tests the complete API workflow: triggering reconciliation,

    querying DynamoDB by job_id, and downloading report from S3.
    """
    payload = {
        "batch_name": "API-Batch-001",
        "records": [
            {
                "transaction_id": "TXN-API-1",
                "order_id": "ORD-1",
                "amount": 250.0,
                "expected_amount": 250.0,
                "currency": "INR",
                "status": "CAPTURED",
                "account_id": "ACT-1",
            },
            {
                "transaction_id": "TXN-API-2",
                "order_id": "ORD-2",
                "amount": 500.0,
                "expected_amount": 400.0,
                "currency": "INR",
                "status": "DISCREPANCY_OVERCHARGED",
                "account_id": "ACT-2",
            },
        ],
    }

    # 1. Trigger job
    response = client.post("/jobs/reconcile", json=payload)
    assert response.status_code == 201
    data = response.json()
    job_id = data["job_id"]
    assert data["status"] == "COMPLETED"
    assert data["summary"]["total_records"] == 2
    assert data["summary"]["discrepancies_detected"] == 1

    # 2. Query DynamoDB job status
    status_resp = client.get(f"/jobs/{job_id}")
    assert status_resp.status_code == 200
    db_record = status_resp.json()
    assert db_record["job_id"] == job_id
    assert db_record["status"] == "COMPLETED"

    # 3. Fetch S3 Report
    report_resp = client.get(f"/jobs/{job_id}/report")
    assert report_resp.status_code == 200
    report = report_resp.json()
    assert report["job_id"] == job_id
    assert len(report["discrepancies"]) == 1


def test_api_query_nonexistent_job(client):
    """Tests 404 response when querying a non-existent job."""
    response = client.get("/jobs/NONEXISTENT-JOB-999")
    assert response.status_code == 404
    assert "not found in DynamoDB" in response.json()["detail"]
