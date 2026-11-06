import argparse
import json
import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage import StorageManager
from src.reconciler import ReconciliationEngine


def execute_with_storage(action_fn):
    """Executes an action either against real AWS/LocalStack or local in-memory simulation."""
    use_real_aws = os.getenv("USE_REAL_AWS", "false").lower() in ("true", "1")

    if not use_real_aws:
        from moto import mock_aws
        with mock_aws():
            storage = StorageManager()
            storage.ensure_resources_exist()
            return action_fn(storage)
    else:
        storage = StorageManager()
        storage.ensure_resources_exist()
        return action_fn(storage)


def main():
    parser = argparse.ArgumentParser(
        description="Amazon OpsPulse CLI - Internal Operational Automation Tool"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. run command
    run_parser = subparsers.add_parser("run", help="Trigger a batch reconciliation workflow")
    run_parser.add_argument("--file", required=True, help="Path to input JSON transactions file")
    run_parser.add_argument("--job-id", default=None, help="Optional custom job identifier")

    # 2. status command
    status_parser = subparsers.add_parser("status", help="Get status of a job from DynamoDB")
    status_parser.add_argument("--job-id", required=True, help="Job identifier to query")

    # 3. report command
    report_parser = subparsers.add_parser("report", help="Fetch audit report from S3")
    report_parser.add_argument("--job-id", required=True, help="Job identifier")

    args = parser.parse_args()

    if args.command == "run":
        def run_action(storage):
            with open(args.file, "r") as f:
                records = json.load(f)
            engine = ReconciliationEngine(storage_mgr=storage)
            print(f"[*] Starting reconciliation workflow for {len(records)} records...")
            result = engine.execute_job(records, job_id=args.job_id)
            print("\n=== Workflow Completed Successfully ===")
            print(f"Job ID:        {result['job_id']}")
            print(f"Final State:   {result['status']}")
            print(f"S3 Report URI: {result['s3_report_url']}")
            print("Summary:")
            print(json.dumps(result["summary"], indent=2, default=str))

        execute_with_storage(run_action)

    elif args.command == "status":
        def status_action(storage):
            item = storage.get_job_execution(args.job_id)
            if not item:
                print(f"[!] Error: Job '{args.job_id}' not found in DynamoDB.")
                sys.exit(1)
            print(f"\n=== DynamoDB Record for {args.job_id} ===")
            print(json.dumps(item, indent=2, default=str))

        execute_with_storage(status_action)

    elif args.command == "report":
        def report_action(storage):
            item = storage.get_job_execution(args.job_id)
            if not item or "s3_report_url" not in item:
                print(f"[!] Error: No S3 report found for job '{args.job_id}'.")
                sys.exit(1)
            report = storage.download_audit_report(item["s3_report_url"])
            print(f"\n=== S3 Operational Audit Report for {args.job_id} ===")
            print(json.dumps(report, indent=2, default=str))

        execute_with_storage(report_action)


if __name__ == "__main__":
    main()
