from contextlib import asynccontextmanager
import logging
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from src.storage import StorageManager
from src.reconciler import ReconciliationEngine

logger = logging.getLogger(__name__)

# Instantiate storage and reconciler engine
storage_manager = StorageManager()
reconciliation_engine = ReconciliationEngine(storage_mgr=storage_manager)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ensure AWS S3 and DynamoDB resources are initialized upon startup."""
    try:
        storage_manager.ensure_resources_exist()
    except Exception as exc:
        logger.warning(f"Could not auto-provision AWS resources on startup: {exc}")
    yield


app = FastAPI(
    title="Amazon OpsPulse: Workflow & Data Reconciliation Engine",
    description="Internal operational service for automated data validation, state tracking in DynamoDB, and audit reports in S3.",
    version="1.0.0",
    lifespan=lifespan,
)


class TransactionItem(BaseModel):
    transaction_id: str
    order_id: str
    amount: float
    expected_amount: float
    currency: str = "INR"
    status: str = "CAPTURED"
    account_id: Optional[str] = "ACT-IND-01"


class TriggerJobRequest(BaseModel):
    batch_name: Optional[str] = "Manual-Ops-Trigger"
    records: List[TransactionItem]


@app.get("/health", tags=["Monitoring"])
def health_check():
    """Service health and AWS configuration status."""
    return {
        "status": "HEALTHY",
        "service": "OpsPulse Reconciliation Engine",
        "aws_region": storage_manager.region_name,
        "s3_bucket": storage_manager.s3_bucket_name,
        "dynamodb_table": storage_manager.dynamodb_table_name,
    }


@app.post("/jobs/reconcile", status_code=status.HTTP_201_CREATED, tags=["Workflows"])
def trigger_reconciliation_job(payload: TriggerJobRequest):
    """Triggers an automated workflow job to reconcile transaction records."""
    records_dict = [item.model_dump() for item in payload.records]
    try:
        result = reconciliation_engine.execute_job(
            records=records_dict,
            job_metadata={"batch_name": payload.batch_name},
        )
        return {
            "message": "Workflow completed successfully",
            "job_id": result["job_id"],
            "status": result["status"],
            "s3_report_url": result["s3_report_url"],
            "summary": result["summary"],
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Reconciliation job failed: {str(exc)}",
        )


@app.get("/jobs/{job_id}", tags=["Workflows"])
def get_job_status(job_id: str):
    """Queries job execution metadata and state from AWS DynamoDB."""
    record = storage_manager.get_job_execution(job_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found in DynamoDB.",
        )
    return record


@app.get("/jobs/{job_id}/report", tags=["Reports"])
def get_job_s3_report(job_id: str):
    """Fetches the complete operational audit report from AWS S3."""
    record = storage_manager.get_job_execution(job_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found in DynamoDB.",
        )

    s3_url = record.get("s3_report_url")
    if not s3_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No S3 report associated with job '{job_id}'.",
        )

    try:
        report = storage_manager.download_audit_report(s3_url)
        return report
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch report from S3: {str(exc)}",
        )
