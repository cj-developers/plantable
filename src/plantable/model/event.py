from datetime import datetime
from typing import List

from .core import _Model

__all__ = ["Activity"]


class Activity(_Model):
    dtable_uuid: str  # "d48c3aae-dbc1-4325-a890-1c2e79ea5319"
    workspace_id: int  # 125
    dtable_name: str  # "SeaTable API Docs"
    dtable_icon: str = None  # "icon-research"
    dtable_color: str = None  # "#1688FC"
    op_date: datetime  # "2020-11-09T09:59:13+00:00"
    insert_row: int = None  # 0
    modify_row: int = None  # 13
    delete_row: int = None  # 0


class ColumnOption(_Model):
    name: str
    color: str
    textColor: str
    id: str


class Column(_Model):
    options: List[ColumnOption]
    value: str = None
    old_value: str = None


class Row(_Model):
    column_key: str
    column_name: str
    column_type: str
    column_data: Column


class Data(_Model):
    dtable_uuid: str
    row_id: str
    op_user: str
    op_type: str
    op_time: float
    table_id: str
    table_name: str
    row_name: str = None
    row_data: List[Row] = None
    op_app: str = None


class Event(_Model):
    event: str
    data: Data
