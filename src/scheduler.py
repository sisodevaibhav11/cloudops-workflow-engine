import logging
from typing import Any, Callable, Dict, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


class WorkflowScheduler:
    """Manages recurring automated operational jobs and batch triggers."""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._is_running = False

    def start(self) -> None:
        """Starts the background scheduler daemon."""
        if not self._is_running:
            self.scheduler.start()
            self._is_running = True
            logger.info("WorkflowScheduler started.")

    def shutdown(self, wait: bool = False) -> None:
        """Safely shuts down the scheduler daemon."""
        if self._is_running:
            self.scheduler.shutdown(wait=wait)
            self._is_running = False
            logger.info("WorkflowScheduler stopped.")

    def add_cron_job(
        self,
        job_func: Callable,
        job_id: str,
        cron_expression: str = "0 2 * * *",  # Default: 02:00 UTC daily
        args: Optional[list] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Registers a recurring cron job for automated operational audits."""
        # Split 5-field cron expression (minute hour day month day_of_week)
        parts = cron_expression.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression '{cron_expression}'. Expected 5 fields.")

        trigger = CronTrigger(
            minute=parts[0],
            hour=parts[1],
            day=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )

        job = self.scheduler.add_job(
            func=job_func,
            trigger=trigger,
            id=job_id,
            args=args or [],
            kwargs=kwargs or {},
            replace_existing=True,
        )
        logger.info(f"Registered scheduled job '{job_id}' with cron '{cron_expression}'")
        return job.id

    def list_jobs(self) -> list:
        """Lists all active scheduled jobs with next run times."""
        return [
            {
                "id": j.id,
                "name": j.name,
                "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
            }
            for j in self.scheduler.get_jobs()
        ]
