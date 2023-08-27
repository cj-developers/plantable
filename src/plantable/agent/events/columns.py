from typing import List

from pydantic import BaseModel, root_validator

from .const import (
    OP_DELETE_COLUMN,
    OP_INSERT_COLUMN,
    OP_MODIFY_COLUMN_METADATA_PERMISSION,
    OP_MODIFY_COLUMN_PERMISSION,
    OP_MODIFY_COLUMN_TYPE,
    OP_RENAME_COLUMN,
    OP_UPDATE_COLUMN_COLORBYS,
    OP_UPDATE_COLUMN_DESCRIPTION,
)
from .model import Column, ColumnMetadataPermission, ColumnPermission, Event


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
    anchor_column_key: str
    column_data: Column
    view_id: str
    rows_datas: list

    @root_validator(pre=True)
    def adjust_column_key(cls, values):
        values["anchor_column_key"] = values["column_key"]
        values.update({"column_key": values["column_data"]["key"]})
        return values


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
class ModifyColumnPermission(ColumnEvent):
    new_column_permission: ColumnPermission
    old_column_permission: ColumnPermission


# Modify Column Metadata Permission
class ModifyColumnMetadataPermission(ColumnEvent):
    new_column_permission: ColumnMetadataPermission
    old_column_permission: ColumnMetadataPermission


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
