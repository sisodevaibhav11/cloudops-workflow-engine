import pytest
from src.scheduler import WorkflowScheduler


def test_scheduler_lifecycle():
    """Verifies starting, adding cron jobs, and shutting down scheduler."""
    scheduler = WorkflowScheduler()

    def dummy_task():
        pass

    scheduler.start()
    assert scheduler._is_running is True

    job_id = scheduler.add_cron_job(
        job_func=dummy_task,
        job_id="daily_reconciliation_audit",
        cron_expression="0 2 * * *",
    )
    assert job_id == "daily_reconciliation_audit"

    jobs = scheduler.list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["id"] == "daily_reconciliation_audit"

    scheduler.shutdown(wait=False)
    assert scheduler._is_running is False


def test_invalid_cron_expression_raises_error():
    """Verifies that an invalid cron expression is caught and rejected."""
    scheduler = WorkflowScheduler()

    def dummy_task():
        pass

    with pytest.raises(ValueError, match="Invalid cron expression"):
        scheduler.add_cron_job(
            job_func=dummy_task,
            job_id="bad_cron_job",
            cron_expression="0 2 *",  # Only 3 fields instead of 5
        )
