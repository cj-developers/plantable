import os

import aioboto3
from dasida import get_secrets
from dotenv import load_dotenv

load_dotenv()

# consts
PROD = "prod"
DEV = "dev"
SEATABLE_VIEW_SUFFIX_TO_WATCH = "__sync"

# AWS S3
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_BUCKET_PREFIX = os.getenv("AWS_S3_BUCKET_PREFIX")

# From Dasida
SM_AWS_S3 = os.getenv("SM_AWS_S3")
if SM_AWS_S3:
    AIOBOTO3_CONF = get_secrets(
        SM_AWS_S3,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
else:
    AIOBOTO3_CONF = None
