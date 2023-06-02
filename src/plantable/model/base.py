from typing import List

from pydantic import BaseModel, validator
from datetime import datetime

from .common import _Model


class Column(_Model):
    key: str  # '0000'
    type: str  # 'text'
    name: str  # 'Hive DB'
    editable: bool  # True
    width: int  # 199
    resizable: bool  # True
    draggable: bool  # True
    data: str = None  # None
    permission_type: str = None  # ''
    permitted_users: List[str] = None  # []
    edit_metadata_permission_type: str = None  # ''
    edit_metadata_permitted_users: List[str] = None  # []
    description: str = None  # None


class Table(_Model):
    _id: str
    name: str
    is_header_locked: bool
    columns: List[Column]
    summary_config: dict = None
    header_settings: dict = None
