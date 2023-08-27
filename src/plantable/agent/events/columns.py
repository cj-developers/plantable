from typing import List

from pydantic import BaseModel
from .const import (
    Event,
    Option,
    OP_INSERT_COLUMN,
    OP_DELETE_COLUMN,
    OP_RENAME_COLUMN,
    OP_UPDATE_COLUMN_DESCRIPTION,
    OP_UPDATE_COLUMN_COLORBYS,
    OP_MODIFY_COLUMN_TYPE,
    OP_MODIFY_COLUMN_PERMISSION,
    OP_MODIFY_COLUMN_METADATA_PERMISSION,
)


class Column(Option):
    rowType: str = None
    key: str  # "4xVF",
    type: str  # "text",
    name: str  # "hello",
    editable: bool  # True,
    width: int  # 200,
    resizable: bool  # True,
    draggable: bool  # True,
    data: dict
    permission_type: str = None  # ""
    permitted_users: list = None  # []
    edit_metadata_permission_type: str  # ""
    edit_metadata_permitted_users: list  # []
    description: str = None
    editor: dict = None
    formatter: dict = None
    idx: int = None
    left: int = None
    last_frozen: bool = None


################################################################
# Models
################################################################
# Column Event
class ColumnEvent(Event):
    op_type: str
    table_id: str
    column_key: str


# Insert Column
class InsertColumn(ColumnEvent):
    column_data: Column
    view_id: str
    rows_datas: list


# Delete Column
class DeleteColumn(ColumnEvent):
    old_column: Column
    upper_column_key: str


# Rename Column
class RenameColumn(ColumnEvent):
    new_column_name: str
    old_column_name: str


# Update Column Description
class UpdateColumnDescription(ColumnEvent):
    column_description: str


# Modify Column Type
class ModifyColumnType(ColumnEvent):
    new_column: Column
    old_column: Column
    new_rows_data: List[dict]
    old_rows_data: List[dict]


# Modify Column Permission
class ColumnPermission(Option):
    permission_type: str
    permitted_users: List[str]


class ModifyColumnPermission(ColumnEvent):
    new_column_permission: ColumnPermission
    old_column_permission: ColumnPermission


# Modify Column Metadata Permission
class MetadataPermission(Option):
    edit_metadata_permission_type: str
    edit_metadata_permitted_users: List[str]


class ModifyColumnMetadataPermission(ColumnEvent):
    new_column_permission: MetadataPermission
    old_column_permission: MetadataPermission


################################################################
# COLUMN PARSER
################################################################
def column_event_parser(data):
    op_type = data["op_type"]

    if op_type == OP_INSERT_COLUMN:
        return [InsertColumn(**data)]

    if op_type == OP_DELETE_COLUMN:
        return [DeleteColumn(**data)]

    if op_type == OP_RENAME_COLUMN:
        return [RenameColumn(**data)]

    if op_type == OP_UPDATE_COLUMN_DESCRIPTION:
        return [UpdateColumnDescription(**data)]

    if op_type == OP_UPDATE_COLUMN_COLORBYS:
        pass

    if op_type == OP_MODIFY_COLUMN_TYPE:
        return [ModifyColumnType(**data)]

    if op_type == OP_MODIFY_COLUMN_PERMISSION:
        return [ModifyColumnPermission(**data)]

    if op_type == OP_MODIFY_COLUMN_METADATA_PERMISSION:
        return [ModifyColumnMetadataPermission(**data)]

    _msg = f"column parser - unknown op_type '{op_type}'!"
    raise KeyError(_msg)
