import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.state_machine import JobState, WorkflowStateMachine
from src.storage import StorageManager

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """Core operational engine that validates batch transaction payloads,

    detects discrepancies/bottlenecks, manages the workflow state machine,
    and publishes audit reports to AWS S3 and DynamoDB.
    """

    def __init__(
        self,
        storage_mgr: StorageManager,
        discrepancy_threshold_pct: float = 20.0,
        max_retries: int = 3,
    ):
        self.storage = storage_mgr
        self.discrepancy_threshold_pct = discrepancy_threshold_pct
        self.max_retries = max_retries

    def execute_job(
        self,
        records: List[Dict[str, Any]],
        job_id: Optional[str] = None,
        job_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Executes the complete operational workflow from ingestion to S3 upload and DynamoDB logging."""
        if not job_id:
            job_id = f"JOB-REC-{uuid.uuid4().hex[:8].upper()}"

        sm = WorkflowStateMachine(initial_state=JobState.PENDING, max_retries=self.max_retries)

        # 1. Initialize record in DynamoDB (PENDING)
        self.storage.save_job_execution(
            job_id=job_id,
            state=sm.current_state.value,
            metadata=job_metadata or {"total_input_records": len(records)},
        )

        try:
            # 2. Transition to RUNNING
            sm.transition_to(JobState.RUNNING)
            self.storage.update_job_status(job_id=job_id, new_state=sm.current_state.value)

            # 3. Transition to VALIDATING
            sm.transition_to(JobState.VALIDATING)
            self.storage.update_job_status(job_id=job_id, new_state=sm.current_state.value)

            # 4. Perform data reconciliation and bottleneck analysis
            audit_result = self.analyze_records(records)

            # 5. Upload full audit report to AWS S3
            report_payload = {
                "job_id": job_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "summary": audit_result["summary"],
                "discrepancies": audit_result["discrepancies"],
                "clean_records_count": audit_result["clean_records_count"],
            }
            s3_uri = self.storage.upload_audit_report(job_id, report_payload)

            # 6. Complete state machine
            final_record = self.storage.update_job_status(
                job_id=job_id,
                new_state=JobState.COMPLETED.value,
                s3_report_url=s3_uri,
                extra_metrics=audit_result["summary"],
            )
            sm.transition_to(JobState.COMPLETED)

            logger.info(f"Reconciliation job {job_id} successfully COMPLETED. S3 URI: {s3_uri}")
            return {
                "job_id": job_id,
                "status": sm.current_state.value,
                "s3_report_url": s3_uri,
                "summary": audit_result["summary"],
                "db_record": final_record,
            }

        except Exception as exc:
            logger.error(f"Error in reconciliation job {job_id}: {exc}")
            failed_state = sm.handle_failure()
            self.storage.increment_retry_count(job_id)
            self.storage.update_job_status(
                job_id=job_id,
                new_state=failed_state.value,
                error_message=str(exc),
            )
            raise

    def analyze_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyzes batch transaction records, detecting amount mismatches,

        unpaid records, and system bottlenecks.
        """
        total = len(records)
        discrepancies = []
        clean_count = 0
        total_settled_amount = 0.0
        total_expected_amount = 0.0

        for rec in records:
            txn_id = rec.get("transaction_id", "UNKNOWN")
            amount = float(rec.get("amount", 0.0))
            expected = float(rec.get("expected_amount", 0.0))
            status = rec.get("status", "UNKNOWN")

            total_settled_amount += amount
            total_expected_amount += expected

            diff = round(amount - expected, 2)
            has_mismatch = diff != 0.0 or status.startswith("DISCREPANCY") or status.startswith("UNPAID")

            if has_mismatch:
                discrepancies.append({
                    "transaction_id": txn_id,
                    "order_id": rec.get("order_id"),
                    "account_id": rec.get("account_id"),
                    "amount": amount,
                    "expected_amount": expected,
                    "discrepancy_delta": diff,
                    "status": status,
                    "issue_type": "OVERCHARGED" if diff > 0 else ("ZERO_VALUED" if amount == 0 else "UNDERCHARGED"),
                })
            else:
                clean_count += 1

        discrepancy_rate = round((len(discrepancies) / total * 100) if total > 0 else 0.0, 2)
        requires_ops_investigation = discrepancy_rate > self.discrepancy_threshold_pct

        summary = {
            "total_records": total,
            "clean_records": clean_count,
            "discrepancies_detected": len(discrepancies),
            "discrepancy_rate_pct": discrepancy_rate,
            "total_settled_amount": round(total_settled_amount, 2),
            "total_expected_amount": round(total_expected_amount, 2),
            "net_variance": round(total_settled_amount - total_expected_amount, 2),
            "ops_alert_triggered": requires_ops_investigation,
        }

        return {
            "summary": summary,
            "discrepancies": discrepancies,
            "clean_records_count": clean_count,
        }
