import os
from dotenv import load_dotenv
import aioboto3
from dasida import get_secrets

load_dotenv()

# PROD and DEV
PROD = "prod"
DEV = "dev"

# AWS S3
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_BUCKET_PREFIX = os.getenv("AWS_S3_BUCKET_PREFIX")

# From Dasida
SM_AWS_S3 = os.getenv("SM_AWS_S3")
aioboto3_conf = get_secrets(
    SM_AWS_S3,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
session = aioboto3.Session(**aioboto3_conf)
