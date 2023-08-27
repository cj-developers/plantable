from typing import Any, List, Union

from pydantic import BaseModel, Extra, Field


# EVENT
class Event(BaseModel):
    class Config:
        extra = Extra.forbid


# OPTION
class Option(BaseModel):
    class Config:
        extra = Extra.forbid


################################################################
# Column
################################################################
class Filter(Option):
    column_key: str
    filter_predicate: str
    filter_term: str


class Sort(Option):
    column_key: str
    sort_type: str


class Groupby(Option):
    column_key: str
    sort_type: str
    count_type: str


################################################################
# Column
################################################################
class Column(Option):
    rowType: str = None
    key: str  # "4xVF",
    type: str  # "text",
    name: str  # "hello",
    editable: bool  # True,
    width: int  # 200,
    resizable: bool  # True,
    draggable: bool  # True,
    data: dict = None
    permission_type: str = None  # ""
    permitted_users: List[str] = None  # []
    edit_metadata_permission_type: str  # ""
    edit_metadata_permitted_users: list  # []
    description: str = None
    editor: dict = None
    formatter: dict = None
    idx: int = None
    left: int = None
    last_frozen: bool = None


class ColumnPermission(Option):
    permission_type: str
    permitted_users: List[str]


class ColumnMetadataPermission(Option):
    edit_metadata_permission_type: str
    edit_metadata_permitted_users: List[str]


################################################################
# View
################################################################
class View(Option):
    id: str = Field(alias="_id")
    name: str
    type: str
    private_for: str = None
    is_locked: bool
    row_height: str
    filter_conjunction: str
    filters: List[dict]
    sorts: List[dict]
    groupbys: List[dict]
    colorbys: dict
    hidden_columns: list
    rows: list
    formula_rows: dict
    link_rows: dict
    summaries: dict
    colors: dict
    column_colors: dict
    groups: list


################################################################
# Table
################################################################
class Table(Option):
    id: str = Field(alias="_id")
    name: str
    is_header_locked: bool
    header_settings: dict
    summary_configs: dict
    columns: List[Column] = None
    rows: List[dict]
    view_structure: dict
    views: List[View] = None
    id_row_map: dict
