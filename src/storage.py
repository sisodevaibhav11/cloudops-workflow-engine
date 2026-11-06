from decimal import Decimal
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

from src import config

logger = logging.getLogger(__name__)


def to_dynamodb_friendly(val: Any) -> Any:
    """Converts float types to Decimal for DynamoDB compatibility."""
    if isinstance(val, float):
        return Decimal(str(val))
    if isinstance(val, dict):
        return {k: to_dynamodb_friendly(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_dynamodb_friendly(v) for v in val]
    return val


class StorageManager:
    """Manages persistence for AWS DynamoDB (job states) and AWS S3 (audit reports)."""

    def __init__(
        self,
        region_name: str = config.AWS_REGION,
        s3_bucket: str = config.S3_BUCKET_NAME,
        dynamodb_table: str = config.DYNAMODB_TABLE_NAME,
        endpoint_url: Optional[str] = config.AWS_ENDPOINT_URL,
    ):
        self.region_name = region_name
        self.s3_bucket_name = s3_bucket
        self.dynamodb_table_name = dynamodb_table
        self.endpoint_url = endpoint_url

        self.s3_client = boto3.client(
            "s3",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
        )
        self.dynamodb = boto3.resource(
            "dynamodb",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
        )
        self._table = None

    def get_table(self):
        """Lazy-loads or returns the DynamoDB Table resource."""
        if self._table is None:
            self._table = self.dynamodb.Table(self.dynamodb_table_name)
        return self._table

    def ensure_resources_exist(self) -> None:
        """Idempotently provisions the S3 bucket and DynamoDB table if they do not exist."""
        # 1. Ensure S3 Bucket exists
        try:
            self.s3_client.head_bucket(Bucket=self.s3_bucket_name)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                create_params: Dict[str, Any] = {"Bucket": self.s3_bucket_name}
                if self.region_name != "us-east-1":
                    create_params["CreateBucketConfiguration"] = {
                        "LocationConstraint": self.region_name
                    }
                self.s3_client.create_bucket(**create_params)
                logger.info(f"Created S3 bucket: {self.s3_bucket_name}")
            else:
                logger.warning(f"S3 head_bucket notice: {exc}")

        # 2. Ensure DynamoDB Table exists
        try:
            table = self.dynamodb.Table(self.dynamodb_table_name)
            table.load()
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("ResourceNotFoundException", "404"):
                logger.info(f"Creating DynamoDB table: {self.dynamodb_table_name}")
                self._table = self.dynamodb.create_table(
                    TableName=self.dynamodb_table_name,
                    KeySchema=[
                        {"AttributeName": "job_id", "KeyType": "HASH"}  # Partition Key
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "job_id", "AttributeType": "S"}
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
                self._table.wait_until_exists()
            else:
                logger.warning(f"DynamoDB load notice: {exc}")

    def save_job_execution(
        self,
        job_id: str,
        state: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Creates an initial job execution record in DynamoDB."""
        now = datetime.now(timezone.utc).isoformat()
        item = {
            "job_id": job_id,
            "status": state,
            "created_at": now,
            "updated_at": now,
            "retry_count": 0,
            "metadata": to_dynamodb_friendly(metadata or {}),
        }
        table = self.get_table()
        table.put_item(Item=item)
        return item

    def get_job_execution(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a job execution record by partition key (job_id)."""
        table = self.get_table()
        response = table.get_item(Key={"job_id": job_id})
        return response.get("Item")

    def update_job_status(
        self,
        job_id: str,
        new_state: str,
        error_message: Optional[str] = None,
        s3_report_url: Optional[str] = None,
        extra_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Atomically updates the job state, timestamp, error, and S3 artifact links."""
        now = datetime.now(timezone.utc).isoformat()
        table = self.get_table()

        update_expr_parts = ["#st = :state_val", "updated_at = :updated_val"]
        expr_attr_names = {"#st": "status"}  # 'status' can be reserved in DynamoDB
        expr_attr_vals: Dict[str, Any] = {
            ":state_val": new_state,
            ":updated_val": now,
        }

        if error_message is not None:
            update_expr_parts.append("error_message = :err_val")
            expr_attr_vals[":err_val"] = error_message

        if s3_report_url is not None:
            update_expr_parts.append("s3_report_url = :s3_val")
            expr_attr_vals[":s3_val"] = s3_report_url

        if extra_metrics:
            update_expr_parts.append("#met = :metrics_val")
            expr_attr_names["#met"] = "metrics"
            expr_attr_vals[":metrics_val"] = to_dynamodb_friendly(extra_metrics)

        update_expression = "SET " + ", ".join(update_expr_parts)

        response = table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_vals,
            ReturnValues="ALL_NEW",
        )
        return response.get("Attributes", {})

    def increment_retry_count(self, job_id: str) -> int:
        """Atomically increments the retry counter in DynamoDB."""
        table = self.get_table()
        response = table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="ADD retry_count :inc",
            ExpressionAttributeValues={":inc": 1},
            ReturnValues="UPDATED_NEW",
        )
        return int(response.get("Attributes", {}).get("retry_count", 0))

    def upload_audit_report(self, job_id: str, report_data: Dict[str, Any]) -> str:
        """Serializes and uploads the operational audit report to AWS S3.

        Returns the full S3 URI: s3://{bucket}/{key}
        """
        key = f"reports/{job_id}/reconciliation_report.json"
        body = json.dumps(report_data, indent=2, default=str)
        self.s3_client.put_object(
            Bucket=self.s3_bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "job_id": job_id,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return f"s3://{self.s3_bucket_name}/{key}"

    def download_audit_report(self, s3_key_or_uri: str) -> Dict[str, Any]:
        """Fetches and parses an audit report from AWS S3."""
        key = s3_key_or_uri
        if key.startswith(f"s3://{self.s3_bucket_name}/"):
            key = key.replace(f"s3://{self.s3_bucket_name}/", "")

        response = self.s3_client.get_object(Bucket=self.s3_bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
