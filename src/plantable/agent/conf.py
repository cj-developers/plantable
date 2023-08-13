import os

from dotenv import load_dotenv
from dasida import get_secrets

load_dotenv()

# SecretsManager Keys
SM_PROFILE_NAME = os.getenv("SM_PROFILE_NAME")
SM_FASTO_ADMIN = os.getenv("SM_FASTO_ADMIN", "fasto-admin")

# Load Secrets from SecretsManager
secrets = get_secrets(SM_FASTO_ADMIN, profile_name=SM_PROFILE_NAME)
SEATABLE_URL = secrets.get("seatable_url") if secrets else None
SEATABLE_USERNAME = secrets.get("seatable_username") if secrets else None
SEATABLE_PASSWORD = secrets.get("seatable_password") if secrets else None
