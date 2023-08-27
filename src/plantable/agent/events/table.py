from typing import Any, List

from pydantic import BaseModel, Field, root_validator

from .const import OP_DELETE_TABLE, OP_INSERT_TABLE, OP_RENAME_TABLE, OP_MODIFY_HEADER_LOCK
from .model import Column, Event, Table, View


################################################################
# Model
################################################################
# Table Event
class TableEvent(Event):
    op_type: str
    table_id: str


# Insert Table
class InsertTable(TableEvent):
    table_data: Table

    @root_validator(pre=True)
    def update_table_id_from_table_data(cls, values):
        values.update({"table_id": values["table_data"]["_id"]})
        return values


# Rename Table
class RenameTable(TableEvent):
    table_name: str


# Delete Table
class DeleteTable(TableEvent):
    table_name: str = None
    deleted_table: Table


# Modify Header Lock
class ModifyHeaderLock(TableEvent):
    is_header_locked: bool


################################################################
# Table Parser
################################################################
def table_event_parser(data):
    op_type = data["op_type"]

    if op_type == OP_INSERT_TABLE:
        return [InsertTable(**data)]

    if op_type == OP_RENAME_TABLE:
        return [RenameTable(**data)]

    if op_type == OP_DELETE_TABLE:
        return [DeleteTable(**data)]

    if op_type == OP_MODIFY_HEADER_LOCK:
        return [ModifyHeaderLock(**data)]

    _msg = f"table_event_parser - unknown op_type '{op_type}'!"
    raise KeyError(_msg)
