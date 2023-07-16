import os
from dotenv import load_dotenv
import aioboto3

load_dotenv()

# Basic Auth for FastApi
AGENT_USERNAME = "admin"
AGENT_PASSWORD = "supersecret"

# AWS S3
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_BUCKET_PREFIX = os.getenv("AWS_S3_BUCKET_PREFIX")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = "ap-northeast-2"

# PROD and DEV
PROD = "prod"
DEV = "dev"

# aioboto3 Session
session = aioboto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION_NAME
)
