import os
import pytest
from moto import mock_aws
from src.storage import StorageManager

# Set test environment credentials
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["S3_BUCKET_NAME"] = "test-amazon-ops-bucket"
os.environ["DYNAMODB_TABLE_NAME"] = "test-OpsJobExecutionTable"


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def storage(aws_credentials):
    """Provides a StorageManager instance hooked into moto's in-memory mock AWS."""
    with mock_aws():
        mgr = StorageManager(
            region_name="us-east-1",
            s3_bucket="test-amazon-ops-bucket",
            dynamodb_table="test-OpsJobExecutionTable",
        )
        mgr.ensure_resources_exist()
        yield mgr
