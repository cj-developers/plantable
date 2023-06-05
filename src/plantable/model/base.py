from datetime import datetime
from enum import Enum
from typing import Any, List

from pydantic import BaseModel, Field, validator

from .core import _Model

__all__ = ["DTABLE_ICON_LIST", "DTABLE_ICON_COLORS", "ColumnType", "Column", "Table"]


DTABLE_ICON_LIST = [
    "icon-worksheet",
    "icon-task-management",
    "icon-software-test-management",
    "icon-design-assignment",
    "icon-video-production",
    "icon-market-analysis",
    "icon-data-analysis",
    "icon-product-knowledge-base",
    "icon-asset-management",
    "icon-financial-information-record",
    "icon-dollar",
    "icon-company-inventory",
    "icon-customer-inquiry",
    "icon-customer-list",
    "icon-product-list",
    "icon-store-address",
    "icon-leave-record",
    "icon-administrative-matters-calendar",
    "icon-customer-relationship",
    "icon-teachers-list",
    "icon-book-library",
    "icon-server-management",
    "icon-time-management",
    "icon-work-log",
    "icon-online-promotion",
    "icon-research",
    "icon-user-interview",
    "icon-client-review",
    "icon-club-members",
]

DTABLE_ICON_COLORS = [
    "#FF8000",
    "#FFB600",
    "#E91E63",
    "#EB00B1",
    "#7626FD",
    "#972CB0",
    "#1DDD1D",
    "#4CAF50",
    "#02C0FF",
    "#00C9C7",
    "#1688FC",
    "#656463",
]


class ColumnType(Enum):
    text = "text"
    long_text = "long-text"
    number = "number"
    collaborator = "collaborator"
    date = "date"
    duration = "duration"
    single_select = "single-select"
    multiple_select = "multiple-select"
    image = "image"
    file = "file"
    email = "email"
    url = "url"
    checkbox = "checkbox"
    rating = "rating"
    formula = "formula"
    link = "link"
    link_formula = "link-formula"
    creator = "creator"
    ctime = "ctime"
    last_modifier = "last-modifier"
    mtime = "mtime"
    auto_number = "auto-number"


class Column(_Model):
    key: str  # '0000'
    type: ColumnType  # 'text'
    name: str  # 'Hive DB'
    editable: bool  # True
    width: int  # 199
    resizable: bool  # True
    draggable: bool = None  # True
    data: Any = None  # None
    permission_type: str = None  # ''
    permitted_users: List[str] = None  # []
    edit_metadata_permission_type: str = None  # ''
    edit_metadata_permitted_users: List[str] = None  # []
    description: str = None  # None


class Table(_Model):
    id: str = Field(..., alias="_id")
    name: str
    is_header_locked: bool = None
    columns: List[Column]
    summary_config: dict = None
    header_settings: dict = None
