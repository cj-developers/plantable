from typing import List

from pydantic import BaseModel


class ColumnOption(BaseModel):
    name: str
    color: str
    textColor: str
    id: str


class Column(BaseModel):
    options: List[ColumnOption]
    value: str = None
    old_value: str = None


class Row(BaseModel):
    column_key: str
    column_name: str
    column_type: str
    column_data: Column


class Data(BaseModel):
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


class Event(BaseModel):
    event: str
    data: Data
