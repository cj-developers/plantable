import os

from dotenv import load_dotenv
from dasida import get_secrets

load_dotenv()

################################################################
# AWS SecretsManager
################################################################
SM_PROFILE = os.getenv("SM_PROFILE", "default")
SM_SEATABLE_ADMIN = os.getenv("SM_SEATABLE_ADMIN")
SM_AWS_S3 = os.getenv("SM_AWS_S3")

################################################################
# Redis Settings
################################################################
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
KEY_PREFIX = os.getenv("KEY_PREFIX", "fasto")

################################################################
# SeaTable Confs
################################################################
SEATABLE_URL = os.getenv("SEATABLE_URL")
SEATABLE_USERNAME = os.getenv("SEATABLE_USERNAME")
SEATABLE_PASSWORD = os.getenv("SEATABLE_PASSWORD")

if SM_SEATABLE_ADMIN:
    secrets = get_secrets(SM_SEATABLE_ADMIN, profile_name=SM_PROFILE)
    if secrets:
        SEATABLE_URL = secrets.get("seatable_url")
        SEATABLE_USERNAME = secrets.get("seatable_username")
        SEATABLE_PASSWORD = secrets.get("seatable_password")

################################################################
# AWS S3 Confs
################################################################
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_BUCKET_PREFIX = os.getenv("AWS_S3_BUCKET_PREFIX", "fasto")

AWS_S3_ACCESS_KEY_ID = os.getenv("AWS_S3_ACCESS_KEY_ID")
AWS_S3_SECRET_ACCESS_KEY = os.getenv("AWS_S3_SECRET_ACCESS_KEY")
AWS_S3_REGION_NAME = os.getenv("AWS_S3_REGION_NAME", "ap-northeast-2")

if SM_AWS_S3:
    secrets = get_secrets(SM_AWS_S3, profile_name=SM_PROFILE)
    if secrets:
        AWS_S3_ACCESS_KEY_ID = secrets.get("aws_access_key_id")
        AWS_S3_SECRET_ACCESS_KEY = secrets.get("aws_secret_access_key")
