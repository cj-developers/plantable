from typing import List

from pydantic import BaseModel, root_validator

from .const import (
    OP_DELETE_VIEW,
    OP_INSERT_VIEW,
    OP_MODIFY_VIEW_TYPE,
    OP_MODIFY_VIEW_LOCK,
    OP_MODIFY_FILTERS,
    OP_MODIFY_GROUPBYS,
    OP_MODIFY_HIDDEN_COLUMNS,
    OP_MODIFY_ROW_COLOR,
    OP_MODIFY_ROW_HEIGHT,
    OP_MODIFY_SORTS,
    OP_RENAME_VIEW,
)
from .model import Event, View, Sort, Filter, Groupby


################################################################
# INSERT VIEW
################################################################
# View Event
class ViewEvent(Event):
    op_type: str
    table_id: str
    view_id: str


# Insert View
class InsertView(ViewEvent):
    view_data: dict
    view_folder_id: str = None

    @root_validator(pre=True)
    def update_view_id_from_table_data(cls, values):
        values.update({"view_id": values["view_data"]["_id"]})
        return values


# Delete View
class DeleteView(ViewEvent):
    view_folder_id: str = None
    view_name: str = None


# Rename View
class RenameView(ViewEvent):
    view_name: str


# Modify View Type
class ModifyViewType(ViewEvent):
    view_type: str


# Modify View Lock
class ModifyViewLock(ViewEvent):
    is_locked: bool


# Modify Filters
class ModifyFilters(ViewEvent):
    filters: List[Filter]
    filter_conjunction: str


# Modify Sorts
class ModifySorts(ViewEvent):
    sorts: List[Sort]


# Modify Groupbys
class ModifyGroupbys(ViewEvent):
    groupbys: List[Groupby]


# Modify Hidden COlumns
class ModifyHiddenColumns(ViewEvent):
    hidden_columns: List[str]


# Modify Row Color
class ModifyRowColor(ViewEvent):
    colorbys: dict


# Modify Row Height
class ModifyRowHeight(ViewEvent):
    row_height: str


################################################################
# View Event Parser
################################################################
def view_event_parser(data):
    op_type = data["op_type"]

    if op_type == OP_INSERT_VIEW:
        return [InsertView(**data)]

    if op_type == OP_DELETE_VIEW:
        return [DeleteView(**data)]

    if op_type == OP_RENAME_VIEW:
        return [RenameView(**data)]

    if op_type == OP_MODIFY_VIEW_TYPE:
        return [ModifyViewType(**data)]

    if op_type == OP_MODIFY_VIEW_LOCK:
        return [ModifyViewLock(**data)]

    if op_type == OP_MODIFY_FILTERS:
        return [ModifyFilters(**data)]

    if op_type == OP_MODIFY_SORTS:
        return [ModifySorts(**data)]

    if op_type == OP_MODIFY_GROUPBYS:
        return [ModifyGroupbys(**data)]

    if op_type == OP_MODIFY_HIDDEN_COLUMNS:
        return [ModifyHiddenColumns(**data)]

    if op_type == OP_MODIFY_ROW_COLOR:
        return [ModifyRowColor(**data)]

    if op_type == OP_MODIFY_ROW_HEIGHT:
        return [ModifyRowHeight(**data)]

    _msg = f"view_parser - unknown op_type '{op_type}'."
    raise KeyError(_msg)
