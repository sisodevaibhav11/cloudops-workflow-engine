from unittest.mock import patch
from moto import mock_aws
from src.cli import main
from src.storage import StorageManager
from src.reconciler import ReconciliationEngine


def test_cli_run_command(capsys):
    """Verifies CLI 'run' command execution with sample payload."""
    with mock_aws():
        test_args = ["cli.py", "run", "--file", "data/sample_payload.json", "--job-id", "CLI-JOB-001"]
        with patch("sys.argv", test_args):
            main()

        captured = capsys.readouterr()
        assert "Workflow Completed Successfully" in captured.out
        assert "CLI-JOB-001" in captured.out
        assert "COMPLETED" in captured.out


def test_cli_status_and_report_commands(capsys):
    """Verifies CLI 'status' and 'report' commands on a completed job."""
    with mock_aws():
        # First execute a job
        storage = StorageManager()
        storage.ensure_resources_exist()
        engine = ReconciliationEngine(storage_mgr=storage)
        engine.execute_job([{"transaction_id": "T1", "amount": 100.0, "expected_amount": 100.0, "status": "CAPTURED"}], job_id="CLI-JOB-002")

        # Test CLI status command
        status_args = ["cli.py", "status", "--job-id", "CLI-JOB-002"]
        with patch("sys.argv", status_args):
            main()

        captured_status = capsys.readouterr()
        assert "DynamoDB Record for CLI-JOB-002" in captured_status.out
        assert "COMPLETED" in captured_status.out

        # Test CLI report command
        report_args = ["cli.py", "report", "--job-id", "CLI-JOB-002"]
        with patch("sys.argv", report_args):
            main()

        captured_report = capsys.readouterr()
        assert "S3 Operational Audit Report for CLI-JOB-002" in captured_report.out
        assert "CLI-JOB-002" in captured_report.out
