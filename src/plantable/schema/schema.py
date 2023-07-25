import logging
from datetime import datetime
from typing import Any, List, Union
from pydantic import BaseModel
import pytz

from ..model import Table

logger = logging.getLogger(__name__)

SCHEMA_MAP = {
    "text": "text",
    "long_text": "long-text",
    "number": "number",
    "collaborator": "collaborator",
    "date": "date",
    "duration": "duration",
    "single_select": "single-select",
    "multiple_select": "multiple-select",
    "image": "image",
    "file": "file",
    "email": "email",
    "url": "url",
    "checkbox": "checkbox",
    "rating": "rating",
    "formula": "formula",
    "link": "link",
    "link_formula": "link-formula",
    "creator": "creator",
    "ctime": "ctime",
    "last_modifier": "last-modifier",
    "mtime": "mtime",
    "auto_number": "auto-number",
}

STR = {"column_type": "text"}
LONGTEXT = {"column_type": "long-text"}
INT = {"column_type": "number", "data": {"format": "number", "decimal": "dot", "thousands": "comma"}}
FLOAT = {"column_type": "number", "data": {"format": "number", "decimal": "dot", "thousands": "comma"}}
DATETIME = {"column_type": "date", "data": {"format": "YYYY-MM-DD HH:mm"}}
BOOL = {"column_type": "checkbox"}

BASE = {
    "table_name": "Base",
    "columns": [
        {"column_name": "base_uuid", **STR},  # "uuid"
        {"column_name": "group_name", **STR},  # "owner"
        {"column_name": "base_name", **STR},  # "name"
        {"column_name": "workspace_id", **INT},
        {"column_name": "created_at", **DATETIME},
        {"column_name": "updated_at", **DATETIME},
        {"column_name": "owner_deleted", **BOOL},
        {"column_name": "rows_count", **INT},
    ],
}

FASTO_USER = {
    "table_name": "FastoUser",
    "columns": {
        {"column_name": "ID", **STR},
        {"column_name": "EmailAddress", **STR},
        {"column_name": "Role", **STR},
        {"column_name": "UserCreated", **DATETIME},
        {"column_name": "LastLogin", **DATETIME},
        {"column_name": "IsActive", **BOOL},
        {"column_name": "Organization", **STR},
        {"column_name": "Division", **STR},
        {"column_name": "DeptCode", **STR},
        {"column_name": "DeptName", **STR},
    },
}
