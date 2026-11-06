import os

# Default AWS environment variables for local testing & cloud deployment
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "amazon-ops-reports-bucket")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "OpsJobExecutionTable")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", None)

# Set dummy credentials if running locally without AWS credentials configured
if not os.getenv("AWS_ACCESS_KEY_ID"):
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_key_amazon_intern"
if not os.getenv("AWS_SECRET_ACCESS_KEY"):
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mock_secret_amazon_intern"
