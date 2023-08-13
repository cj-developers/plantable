import os

from dotenv import load_dotenv

load_dotenv()

# Seatable
SEATABLE_URL = os.getenv("SEATABLE_URL")
SEATABLE_USERNAME = os.getenv("SEATABLE_USERNAME")
SEATABLE_PASSWORD = os.getenv("SEATABLE_PASSWORD")
SEATABLE_ACCOUNT_TOKEN = os.getenv("SEATABLE_ACCOUNT_TOKEN")
SEATABLE_API_TOKEN = os.getenv("SEATABLE_API_TOKEN")
